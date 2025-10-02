import xarray as xr, pandas as pd, numpy as np
import tqdm.auto as tqdm
from pathlib import Path
from pydantic import BaseModel
from typing import Literal, Dict, List, Union, Any
import scipy.signal
from dask.diagnostics import ProgressBar
import dask
from sonpy import lib as sp
import builtins
from .utilities import dask_array_from_chunk_function
from collections.abc import Iterator
from contextlib import contextmanager
import re

@contextmanager
def sonfile(path: Path):
    rec = sp.SonFile(str(path), True)
    try:
        yield rec
    finally:
        pass
        # del rec
        # rec.close()

def smrxchanneldata(smrx_path: Path) -> pd.DataFrame:
    with sonfile(smrx_path) as rec:
        time_base = rec.GetTimeBase()
        spike2channels = []
        for i in range(rec.MaxChannels()):
            chan_name = rec.GetChannelTitle(i)
            chan_type = str(rec.ChannelType(i))
            physical_channel = rec.PhysicalChannel(i)
            start_t = rec.FirstTime(i, 0, rec.ChannelMaxTime(i)) * time_base
            end_t = rec.ChannelMaxTime(i) *time_base
            chan_num = i
            if chan_type != "DataType.Off":
                if chan_type == "DataType.Adc":
                    divide = rec.ChannelDivide(i)
                    fs = float(1/(divide*time_base))
                else:
                    fs = -1
                spike2channels.append(dict(chan_name=chan_name, chan_type=chan_type[len("DataType."):], physical_channel=physical_channel, chan_num=chan_num, fs=fs, start_t=start_t, end_t=end_t))
    return pd.DataFrame(spike2channels, columns=["chan_name", "chan_type", "physical_channel", "chan_num", "fs", "start_t", "end_t"])


@contextmanager
def smrxadc2electrophy(smrx_path: Path, channel_nums: List[Union[int, Any]] = [...]) -> Iterator[xr.DataArray]:
    channel_metadata = smrxchanneldata(smrx_path)
    if ... in channel_nums:
        channel_nums = [c for c in channel_metadata["chan_num"].loc[(channel_metadata["chan_type"] == "Adc")].tolist() if not c in channel_nums]
    channel_metadata = channel_metadata.loc[channel_metadata["chan_num"].isin(channel_nums)]
    if channel_metadata.empty:
        a= xr.DataArray(np.zeros((0, 0), dtype=float), dims=["channel", "t"], name="data")
        a["t"] = xr.DataArray(np.zeros((0), dtype=float), dims=["t"])
        a["t"].attrs["fs"] = 1000
        a["channel"] = xr.DataArray(np.full((0), 0, dtype=int), dims=["channel"])
        # a["chan_name"] = xr.DataArray(np.full((0), "chan", dtype=str), dims=["channel"])
        yield a
        return 
    if ("Adc" != channel_metadata["chan_type"]).any():
        raise Exception("Not all channels are Adc")
    fs, start_t, end_t = channel_metadata["fs"].iat[0], channel_metadata["start_t"].max(), channel_metadata["end_t"].min()
    if (fs != channel_metadata["fs"]).any():
        raise Exception("Not all channels same fs")
    n_elems = int(np.round((end_t - start_t)*fs))

    with sonfile(smrx_path) as rec:
        time_base = rec.GetTimeBase()
        divide = int(np.round(1/(fs * time_base)))
        def retrieve_data(slices: List[slice]):
            channel_slice, time_slice = slices
            time_size = time_slice.stop-time_slice.start
            res = []
            for chan_ind in range(channel_slice.start, channel_slice.stop):
                chan = channel_metadata["chan_num"].iat[chan_ind]
                chunk = np.array(rec.ReadFloats(chan, time_size, divide*slices[1].start+ int(np.round(start_t/time_base))))
                if chunk.size < time_size:
                    raise Exception("Smaller")
                elif chunk.size > time_size:
                    raise Exception("Bigger")
                res.append(chunk)
            res = np.stack(res, axis=0)
            return res
        
        a = xr.DataArray(dask_array_from_chunk_function(retrieve_data, (len(channel_nums), n_elems), (1, 10**7), float), dims=["channel", "t"], name="data")
        a["t"] = np.arange(n_elems)/fs + start_t
        a["t"].attrs["fs"] = fs
        a["channel"] = channel_metadata["chan_num"]
        # a["chan_name"] = xr.DataArray(channel_metadata["chan_name"].to_numpy(), dims=["channel"])
        yield a


    
