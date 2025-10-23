import pandas as pd
import xarray as xr, pandas as pd, numpy as np, json, datetime
from dateutil import parser
from pathlib import Path
from typing import List, Dict, Any
import dask, dask.array as da, tqdm.auto as tqdm

def fiber2events(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["t"] = df["TimeStamp"]/1000
    if not df["t"].is_monotonic_increasing:
        raise Exception("Data should already be sorted")
    df = df.reset_index()

    res = []
    for n, g in df.groupby("Name"):
        starts_arr = g.loc[g["State"] == 0, "t"].to_numpy()
        ends_arr = g.loc[g["State"] == 1, "t"].to_numpy()
        start_indices = g.loc[g["State"] == 0, "index"].to_numpy()

        if starts_arr.size > 0 and ends_arr.size > 0:
            if starts_arr[0] > ends_arr[0]:
                starts_arr = np.insert(starts_arr, 0, np.nan)
                start_indices = np.insert(start_indices, 0, -1)
            if starts_arr[-1] > ends_arr[-1]:
                ends_arr = np.append(ends_arr, np.nan)

        if starts_arr.shape != ends_arr.shape:
            raise Exception("Not same number of rise and fall events")
        durations = ends_arr - starts_arr
        if (durations < 0 ).any():
            raise Exception("Problem aligning rise and falls events")
        result = pd.DataFrame()
        result["start"] = starts_arr
        result["duration"] = durations
        result["event_name"] = n
        result["index"] = start_indices
        res.append(result)

    final_df = pd.concat(res).sort_values("index")[["event_name", "start", "duration"]].reset_index(drop=True)
    return final_df


def lfp2xr(lfp: dict) -> List[xr.Dataset]:
    date = parser.parse(lfp["SessionDate"]).astimezone(tz=None)
    sigs = []
    for d in lfp["IndefiniteStreaming"] if "IndefiniteStreaming" in lfp else lfp["BrainSenseTimeDomain"]:
        channel = d["Channel"]
        a = xr.DataArray(d["TimeDomainData"], dims="t")
        fs = d["SampleRateInHz"]
        a["t"] = np.arange(a.sizes["t"])/fs
        a["t"].attrs["fs"] = fs
        adate = str(date + datetime.timedelta(seconds=d["FirstPacketDateTimeOffsetInSeconds"]))
        sigs.append(dict(ar=a, channel=channel, date=adate))

    def make_ds(g: pd.DataFrame) -> xr.Dataset:
        ds = xr.Dataset()
        for _, row in g.iterrows():
            ds[row["channel"]] = row["ar"]
        
        ret =  ds.to_array(dim="channel").to_dataset(name="data")
        ret.attrs["segdate"] = g["date"].iat[0]
        return ret

    all_ds = []
    for i, (_, g) in enumerate(pd.DataFrame(sigs).groupby("date", sort=True)):
        ds = make_ds(g).assign_attrs(segnum=i)
        all_ds.append(ds)

    return all_ds

import mne.io.edf.edf
def eeg2xr(eeg_data: mne.io.edf.edf.RawEDF) -> xr.Dataset:
    EEG_chans = [d["ch_name"] for d in eeg_data.info["chs"] if d["kind"] ==2]
    times = eeg_data.times

    read_progress = tqdm.tqdm(desc="Reading mne file")

    def read_chunk(chan_index, t_index, chunk_size):
        ret, _ = eeg_data[EEG_chans[chan_index], t_index:t_index+chunk_size]
        read_progress.update()
        return ret

    channel_chunks = []
    n_chunks=0
    for chan_index in range(0, len(EEG_chans), 1):
        time_chunks = []
        for t_index in range(0, times.size, 10**6):
            n_chunks+=1
            chunk_size = min(10**6, times.size - t_index)
            chunk = da.from_delayed(dask.delayed(read_chunk)(chan_index, t_index, chunk_size),
                shape=(1, ) + (chunk_size, ),
                dtype=float
            )
            time_chunks.append(chunk)
        channel_chunks.append(da.concatenate(time_chunks, axis=1))
    darr = da.concatenate(channel_chunks, axis=0)
    read_progress.total = n_chunks

    ds = xr.Dataset()
    ds["channel"] = xr.DataArray(EEG_chans, dims="channel")
    ds["t"] = xr.DataArray(times, dims="t")
    ds["data"] = xr.DataArray(darr, dims=["channel", "t"])

    return ds