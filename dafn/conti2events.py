import xarray as xr, pandas as pd, numpy as np
from scipy.stats import gaussian_kde
from scipy.signal import find_peaks
from typing import Tuple
import plotly.express as px
import plotly.graph_objects as go

class ThresholdInfo:
    def __init__(self, info):
        self.info = info

def get_threshold_np(arr: np.ndarray, npoints = 1000, quantiles = (0.0001, 0.9999), return_info: None = False) -> float | Tuple[float, ThresholdInfo]:
        figure_info = []
        positive_peaks, _ = find_peaks(arr)
        negative_peaks, _ = find_peaks(-arr)
        arr = arr[np.concatenate([positive_peaks, negative_peaks])]
        quantiles = np.quantile(arr, list(quantiles))
        arr = arr[(arr >=quantiles[0]) & (arr <=quantiles[1])]
        if arr.size > 10**5:
            arr = np.random.default_rng().choice(arr, 10**5)
        min = np.min(arr)
        max = np.max(arr)
        eval = np.linspace(min, max, npoints, endpoint=True)
        bw_value = arr.size**(-1./5)
        min_bw = 0
        max_bw = None
        n_tries = 0
        last_good_thresh = None
        while n_tries < 8:
            n_tries+=1
            dist = gaussian_kde(arr, bw_method=bw_value)(eval)
            peaks, _ = find_peaks(dist)
            peaks = list(peaks)
            if dist[0] > dist[1]:
                peaks = [0] + peaks
            if dist[-1] > dist[-2]:
                peaks =peaks + [-1]
            peaks = np.array(peaks)
            figure_info.append(dict(iteration=n_tries, bw=bw_value, x_times=eval, dist=dist, peaks_indices=peaks))
            if peaks.size > 2:
                min_bw = bw_value
                if max_bw is None:
                    bw_value*=2
                else:
                    bw_value= (bw_value+max_bw)/2
            elif peaks.size == 2:
                pos = dist[peaks[0]:peaks[1]].argmin()
                last_good_thresh = eval[pos + peaks[0]]
                max_bw = bw_value
                bw_value= (bw_value+min_bw)/2
            else:
                max_bw = bw_value
                bw_value= (bw_value+min_bw)/2
        if return_info:
            return last_good_thresh, ThresholdInfo(figure_info)
        else:
            return last_good_thresh

def get_thresholds(arr: xr.DataArray, per_channel: bool = True, npoints:int = 1000, quantiles = (0.0001, 0.9999), return_info: bool = False) -> xr.DataArray:
    if per_channel:
        result = xr.apply_ufunc(get_threshold_np, arr, input_core_dims=[["t"]], output_core_dims=[[], []] if return_info else [[]], vectorize=True,
                                 kwargs=dict(npoints=npoints, quantiles=quantiles, return_info=return_info))
        if return_info:
            ret, info_arr = result
            info = []
            for ch_num in range(info_arr.sizes["channel"]):
                ch_label = arr["channel"].isel(channel=ch_num).item()
                ti: ThresholdInfo = info_arr.isel(channel=ch_num).item()
                for d in ti.info:
                    info.append(dict(channel=ch_label, **d))
        else:
            ret = result
    else:
        result = get_threshold_np(arr.to_numpy().flatten(), npoints, quantiles, return_info=return_info)
        if return_info:
            ret, info = result
            info = [dict(channel="all", **d) for d in info.info]
        else:
            ret = result
        ret = xr.ones_like(arr["channel"]) * ret
    if return_info:
        return ret, info
    else:
        return ret

def make_threshold_fig(figure_info) -> go.Figure:  
    dfs = []
    for fi in figure_info:
        df = pd.DataFrame()
        df["amp"] = fi["x_times"]
        df["kde"] = fi["dist"]
        df["bw"] = fi["bw"]
        df["iteration"] = fi["iteration"]
        df["channel"] = fi["channel"]
        df["n_peaks"] = len(fi["peaks_indices"])
        dfs.append(df)
    dist = pd.concat(dfs)
    fig = px.line(dist, x="amp", y="kde", facet_row="channel", color="bw", line_dash="n_peaks", hover_data=["bw", "n_peaks", "iteration"])
    return fig

def continuous_to_events(input: xr.DataArray, threshold: xr.DataArray, min_distance: None | float = None) -> pd.DataFrame:
    ds = xr.Dataset()
    ds["input"] = input
    ds["thresh"] = threshold
    ds["binary"] = ds["input"] > ds["thresh"]
    ds["is_rising"] = ds["binary"] > ds["binary"].shift(t=1, fill_value=True) 
    ds["is_falling"] = ds["binary"].shift(t=-1, fill_value=True) < ds["binary"]


    result = []
    for chan in range(ds["channel"].size):
        d = ds.isel(channel=chan)

        def get_time_intersect_thresh(indice_above, indices_under):
            x1 = d["t"].isel(t=indices_under).to_numpy()
            y1 = d["input"].isel(t=indices_under).to_numpy()
            x2 = d["t"].isel(t=indice_above).to_numpy()
            y2 = d["input"].isel(t=indice_above).to_numpy()
            prop =(d["thresh"].item() - y1)/(y2 - y1)
            final_x = x1 + (x2 - x1)*prop
            return final_x
        
        rise_indices = np.flatnonzero(d["is_rising"].to_numpy())
        rise_times = get_time_intersect_thresh(rise_indices, rise_indices-1)
        fall_indices = np.flatnonzero(d["is_falling"].to_numpy())
        fall_times = get_time_intersect_thresh(fall_indices, fall_indices+1)

        if d["binary"].isel(t=0):
            rise_times = np.insert(rise_times, 0, np.nan)

        if d["binary"].isel(t=-1):
            fall_times = np.append(fall_times, np.nan)
        
        if rise_times.size != fall_times.size:
            raise Exception("Not same size...")
        if min_distance:
            new_rise_times = [rise_times[0]]
            new_fall_times = []

            for i in range(len(rise_times)-1):
                if (rise_times[i+1] - fall_times[i]) > min_distance:
                    new_fall_times.append(fall_times[i])
                    new_rise_times.append(rise_times[i+1])

            new_fall_times.append(fall_times[-1])
            rise_times = np.array(new_rise_times)
            fall_times = np.array(new_fall_times)

        duration = fall_times - rise_times
        df = pd.DataFrame().assign(start=rise_times, duration=duration, event_name=d["channel"].item())
        result.append(df)
    result = pd.concat(result, ignore_index=True)[["event_name", "start", "duration"]]
    return result

def plot_continuous_to_events(inputs, thresholds, events):
    ds = xr.Dataset()
    ds["input"] = inputs
    ds["thresh"] = thresholds

    if ds.sizes["t"] > 10**5:
        plot_ds = ds.coarsen(t=int(ds.sizes["t"] / (10**5)), boundary="trim").max()
    else:
        plot_ds = ds

    from plotly.subplots import make_subplots
    fig = make_subplots(rows=inputs["channel"].size, cols=1, shared_xaxes=True)

    for chan in range(plot_ds["channel"].size):
        chan_ds = plot_ds.isel(channel=chan)
        m, M = chan_ds["input"].min().item(), chan_ds["input"].max().item()
        fig.add_trace(go.Scatter(x=chan_ds["t"], y=chan_ds["input"], name=chan_ds["channel"].item(), opacity=0.5), row=chan+1, col=1)
        fig.add_hline(y=chan_ds["thresh"].item(), line_dash="dot",
                    annotation_text=str(chan_ds["channel"].item()), 
                    annotation_position="bottom right", row=chan+1, col=1)
        
        sub_df: pd.DataFrame = events.loc[events["event_name"] == chan_ds["channel"].item()]
        for _, row in sub_df.iterrows():
            fig.add_trace(
            go.Scatter(
                x=[row["start"], row["start"] + row["duration"], row["start"] + row["duration"], row["start"]], 
                y=[m, m, M, M],
                fill="toself",
                fillcolor="pink",
                opacity=0.5,
                mode="lines",
                line=dict(width=0), showlegend=False), row=chan+1, col=1)
    return fig

        
        