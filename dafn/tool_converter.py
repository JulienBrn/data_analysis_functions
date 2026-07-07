import datetime
import re
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import dask
import dask.array as da
import numpy as np
import pandas as pd
import tqdm.auto as tqdm
import xarray as xr
from dateutil import parser
from pydantic import BaseModel

if TYPE_CHECKING:
    import mne.io.edf.edf
    import networkx as nx


def fiber2events(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["t"] = df["TimeStamp"] / 1000
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
        if (durations < 0).any():
            raise Exception("Problem aligning rise and falls events")
        result = pd.DataFrame()
        result["start"] = starts_arr
        result["duration"] = durations
        result["event_name"] = n
        result["index"] = start_indices
        res.append(result)

    final_df = pd.concat(res).sort_values("index")[["event_name", "start", "duration"]].reset_index(drop=True)
    return final_df


def mne2events(eeg_data: "mne.io.edf.edf.RawEDF") -> pd.DataFrame:
    import mne

    df = pd.DataFrame(mne.find_events(eeg_data, initial_event=True, output="step", consecutive=True), columns=["index", "from", "to"])
    df["t"] = eeg_data.times[df["index"]]
    df = df.copy()
    df = df[["t", "from", "to"]]

    df["from_next"] = df["from"].shift(-1, fill_value=0)
    df["t_next"] = df["t"].shift(-1)
    if (df["from_next"] != df["to"]).any():
        raise Exception("events are not isolated")
    df["duration"] = df["t_next"] - df["t"]
    df["event_name"] = df["to"]
    df["start"] = df["t"]
    df = df[["event_name", "start", "duration"]]

    zero_df = df.iloc[1::2, :]
    if (zero_df["event_name"] != 0).any():
        raise Exception("events do not always go back to 0")
    final_df = df.iloc[::2, :]
    if (final_df["event_name"] == 0).any():
        raise Exception("non-events found...")

    return final_df


def lfp2xr(lfp: dict) -> list[xr.Dataset]:
    date = parser.parse(lfp["SessionDate"]).astimezone(tz=None)
    sigs = []
    for d in lfp["IndefiniteStreaming"] if "IndefiniteStreaming" in lfp else lfp["BrainSenseTimeDomain"]:
        channel = d["Channel"]
        a = xr.DataArray(d["TimeDomainData"], dims="t")
        fs = d["SampleRateInHz"]
        a["t"] = np.arange(a.sizes["t"]) / fs
        a["t"].attrs["fs"] = fs
        adate = str(date + datetime.timedelta(seconds=d["FirstPacketDateTimeOffsetInSeconds"]))
        sigs.append(dict(ar=a, channel=channel, date=adate))

    def make_ds(g: pd.DataFrame) -> xr.Dataset:
        ds = xr.Dataset()
        for _, row in g.iterrows():
            ds[row["channel"]] = row["ar"]

        ret = ds.to_array(dim="channel").to_dataset(name="data")
        ret.attrs["segdate"] = g["date"].iat[0]
        return ret

    all_ds = []
    for i, (_, g) in enumerate(pd.DataFrame(sigs).groupby("date", sort=True)):
        ds = make_ds(g).assign_attrs(segnum=i)
        all_ds.append(ds)

    return all_ds


def eeg2xr(eeg_data: "mne.io.edf.edf.RawEDF") -> xr.Dataset:
    EEG_chans = [d["ch_name"] for d in eeg_data.info["chs"] if d["kind"] == 2]
    times = eeg_data.times

    read_progress = tqdm.tqdm(desc="Reading mne file")

    def read_chunk(chan_index, t_index, chunk_size):
        ret, _ = eeg_data[EEG_chans[chan_index], t_index : t_index + chunk_size]
        read_progress.update()
        return ret

    channel_chunks = []
    n_chunks = 0
    for chan_index in range(0, len(EEG_chans), 1):
        time_chunks = []
        for t_index in range(0, times.size, 10**6):
            n_chunks += 1
            chunk_size = min(10**6, times.size - t_index)
            chunk = da.from_delayed(dask.delayed(read_chunk)(chan_index, t_index, chunk_size), shape=(1,) + (chunk_size,), dtype=float)
            time_chunks.append(chunk)
        channel_chunks.append(da.concatenate(time_chunks, axis=1))
    darr = da.concatenate(channel_chunks, axis=0)
    read_progress.total = n_chunks

    ds = xr.Dataset()
    ds["channel"] = xr.DataArray(EEG_chans, dims="channel")
    ds["t"] = xr.DataArray(times, dims="t")
    ds["data"] = xr.DataArray(darr, dims=["channel", "t"])

    return ds


def polydat2df(dat_path: Path, task_path: Path | None):
    dat_df = pd.read_csv(
        Path(dat_path),
        sep="\t",
        names=["time (ms)", "family", "nbre", "_P", "_V", "_L", "_R", "_T", "_W", "_X", "_Y", "_Z"],
        skiprows=13,
        dtype=int,
    )
    dat_df["t"] = dat_df.pop("time (ms)") / 1000
    dat_df["curr_node"] = dat_df["_T"].where(dat_df["family"] == 10).ffill()
    if task_path:
        task_df = polyex2df(task_path)
        names = []
        pat = r"(?P<name>\D+\d*)" + re.escape(r"(") + r"(?P<family>\d+),(?P<nbre>\d+)" + re.escape(")")
        for c in task_df.columns:
            m = re.match(pat, c)
            if m:
                names.append(m.groupdict())
        names.append(dict(name="LINE_CHANGE", family="10", nbre="1"))
        names = pd.DataFrame(names)
        for col in ["family", "nbre"]:
            names[col] = names[col].replace("", None).astype(pd.Int64Dtype())

        event_df = pd.merge(dat_df, names, on=["family", "nbre"], how="inner")
        return event_df
    return dat_df


def polyex2df(task_path: Path | str) -> pd.DataFrame:

    task_path = Path(task_path)
    with Path(task_path).open("r") as f:
        i = 0
        while f:
            l = f.readline().split("\t")
            if len([x for x in l if "NEXT" in x]) > 1:
                break
            i += 1
    task_df = pd.read_csv(task_path, sep="\t", skiprows=i)
    task_df = task_df.rename(columns={task_df.columns[0]: "task_node"})
    return task_df


def polyex2graph(task_path: Path | str) -> "nx.DiGraph":
    import networkx as nx

    task_df = polyex2df(task_path)
    df = task_df
    df = df.loc[~pd.isna(df["task_node"])]
    df = df.dropna(subset=df.columns[1:], how="all")
    df["task_node"] = df["task_node"].astype(int)
    graph = nx.DiGraph()
    for _, row in df.iterrows():
        row = row.dropna().to_dict()
        names = []
        graph.add_node(row["task_node"])
        node = graph.nodes[row["task_node"]]
        for col in row:
            if col.startswith("NEXT"):
                pattern = r"\(.+\)$"
                ns = re.findall(pattern, row[col])
                if len(ns) == 0:
                    next_line = row["task_node"] + 1
                    cond = row[col]
                elif len(ns) == 1:
                    cond = row[col][: -len(ns[0])]
                    nlname = ns[0][1:-1]
                    if re.match(r"\d+", nlname):
                        next_line = int(nlname)
                    else:
                        next_line = df.loc[(df[["T1", "T2", "T3"]].apply(lambda s: s.str.lstrip("_")) == nlname).any(axis=1)]["task_node"]
                        if len(next_line) != 1:
                            raise Exception(f"problem {len(next_line)} {nlname}")
                        next_line = next_line.iat[0]
                else:
                    raise Exception("Problem")
                graph.add_edge(row["task_node"], next_line, cond=cond)
            elif re.match(r"T\d+", col):
                m = re.match(r"(?P<time>\d*-?\d*)_(?P<name>\w+)$", str(row[col]))
                if m is not None:
                    names.append(m["name"])
                    if m["time"]:
                        node[col] = m["time"]
                else:
                    node[col] = row[col]
            else:
                node[col] = row[col]
        node["poly_names"] = names
    return graph


class BinaryFamilyProcessInfo(BaseModel):
    kind: Literal["binary"] = "binary"
    reverse: bool = False
    nbre_filter: list[int] = []

    def handle_group(self, group, state_col, name):
        group = group.loc[group[state_col] != group[state_col].shift(1)].copy()
        group[state_col] = group[state_col].astype(bool)
        if self.reverse:
            group[state_col] = ~group[state_col]
        if group[state_col].iat[0] == 0:
            group = group.iloc[1:, :]
        if len(group.index) == 0:
            return pd.DataFrame([], columns=["start", "duration", "event_name", "start_node", "end_node"])
        if (group[state_col].iloc[::2] != 1).any():
            print(group)
            raise Exception("Problem")
        if (group[state_col].iloc[1::2] != 0).any():
            print(group)
            raise Exception("Problem")
        rises = group["t"].iloc[::2]
        falls = group["t"].iloc[1::2].tolist()
        start_node = group["curr_node"].iloc[::2]
        end_node = group["curr_node"].iloc[1::2].tolist()
        if group[state_col].iat[-1] != 0:
            falls += [np.nan]
            end_node += [None]
        return pd.DataFrame().assign(start=rises, duration=falls - rises, event_name=name, start_node=start_node, end_node=end_node)


class EventFamilyProcessInfo(BaseModel):
    kind: Literal["event"] = "event"
    nbre_filter: list[int] = []

    def handle_group(self, group, state_col, name):
        group = group.loc[group[state_col].astype(bool)].copy()
        rises = group["t"]
        start_node = group["curr_node"]
        return pd.DataFrame().assign(start=rises, duration=np.nan, event_name=name, start_node=start_node, end_node=np.nan)


basic_poly_processing_family = {
    1: {"_P": BinaryFamilyProcessInfo()},
    2: {"_V": BinaryFamilyProcessInfo()},
    5: {"_P": EventFamilyProcessInfo()},
    6: {"_P": BinaryFamilyProcessInfo(reverse=True), "_V": BinaryFamilyProcessInfo(reverse=True, nbre_filter=[20])},
    13: {"_P": BinaryFamilyProcessInfo()},
    15: {"_P": BinaryFamilyProcessInfo()},
    10: {"_T": EventFamilyProcessInfo()},
}


def convert2events(
    event_df: pd.DataFrame,
    processing: dict[int, dict[str, BinaryFamilyProcessInfo | EventFamilyProcessInfo]] = basic_poly_processing_family,
):
    results = []
    for (n, f, nb), group in event_df.groupby(["name", "family", "nbre"]):
        if f not in processing:
            raise Exception(f"Unhandled family {f}")
        state_col = {k: v for k, v in processing[f].items() if nb not in v.nbre_filter}
        if len(state_col) > 1:
            for s, g in state_col.items():
                results.append(g.handle_group(group, s, n + s))
        elif len(state_col) == 1:
            for s, g in state_col.items():
                results.append(g.handle_group(group, s, n))

    results = pd.concat(results, ignore_index=True)
    return results


def blackrock2xr(nsx_file: "brpylib.nsx_file.NsxFile") -> xr.Dataset:
    nsx_data = nsx_file.getdata("all", 0, "all", 1, full_timestamps=True)
    ext_headers = pd.DataFrame(nsx_file.extended_headers).to_xarray().set_coords("ElectrodeID").rename(ElectrodeID="elec_id").drop_vars("index").rename(index="channel")
    all_data = nsx_data["data"][0]

    def read_chunk(chan_index, t_index, chunk_size):
        ret = np.asarray(all_data[chan_index : chan_index + 1, t_index : t_index + chunk_size])
        return ret

    channel_chunks = []
    n_chunks = 0
    for chan_index in range(0, all_data.shape[0], 1):
        time_chunks = []
        for t_index in range(0, all_data.shape[1], 10**6):
            n_chunks += 1
            chunk_size = min(10**6, all_data.shape[1] - t_index)
            chunk = da.from_delayed(
                dask.delayed(read_chunk)(chan_index, t_index, chunk_size),
                shape=(1,) + (chunk_size,),
                dtype=all_data.dtype,
            )
            time_chunks.append(chunk)
        channel_chunks.append(da.concatenate(time_chunks, axis=1))
    darr = da.concatenate(channel_chunks, axis=0)
    d = xr.Dataset()
    arr = xr.DataArray(darr, dims=["channel", "t"])
    arr["t"] = np.arange(all_data.shape[1]) / nsx_file.basic_header["SampleResolution"]
    arr["t"].attrs["fs"] = float(nsx_file.basic_header["SampleResolution"])
    d["data"] = arr
    d["elec_id"] = xr.DataArray(nsx_data["elec_ids"], dims="channel")
    d = d.set_coords(["elec_id"])
    d = xr.merge([d, ext_headers])
    d.attrs["comment"] = str(nsx_file.basic_header["Comment"])
    d.attrs["recording_date"] = str(nsx_file.basic_header["TimeOrigin"])
    d["channel"] = d["elec_id"]
    d = d.drop_vars("elec_id")
    return d
