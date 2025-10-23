import xarray as xr, pandas as pd, numpy as np
from pydantic import BaseModel
import scipy.signal, re
from abc import ABC, abstractmethod
from typing import Literal, Union, List, Callable, Any, Hashable, Optional
import dask.array as da

class Filter(ABC):
    @abstractmethod
    def get_sos_filter(self, signal_fs: float): ...

    @abstractmethod
    def get_distrust_time(self): ...


class LowpassFilter(Filter, BaseModel):
     order: int = 4
     freq: float

     def get_sos_filter(self, signal_fs):
         return scipy.signal.butter(self.order, self.freq, btype="lowpass", output="sos", fs=signal_fs)
     def get_distrust_time(self):
         return 10/self.freq

class BandpassFilter(Filter, BaseModel):
    order: int = 4
    low_freq: float
    high_freq: float

    def get_sos_filter(self, signal_fs):
         return scipy.signal.butter(self.order, [self.low_freq, self.high_freq], btype="bandpass", output="sos", fs=signal_fs)
    def get_distrust_time(self):
        return 10/self.low_freq
    
def _compute_signal_processing(ar: xr.DataArray, func, out_freq: float, distrust_time: float):
    ar = ar.chunk({k:1 if k!= "t" else -1 for k in ar.dims})
    min_t = int(np.ceil((ar["t"].min().item()+distrust_time)*out_freq))
    max_t = int(np.floor((ar["t"].max().item()-distrust_time)*out_freq))
    final_t = xr.DataArray(np.arange(min_t, max_t+1)/out_freq, dims="t")
    res: xr.DataArray = xr.apply_ufunc(func, ar, input_core_dims=[["t"]], output_core_dims=[["t"]], dask="parallelized", output_dtypes=[float])
    interped = res.interp(t=final_t)
    interped["t"].attrs["fs"]=out_freq
    return interped

def compute_lfp(ar: xr.DataArray, lowpass_filter: LowpassFilter = LowpassFilter(freq=300), out_freq: float = 1000) -> xr.DataArray:
    distrust_time = lowpass_filter.get_distrust_time()
    low_filter = lowpass_filter.get_sos_filter(ar["t"].attrs["fs"])

    def compute_lfp(a: np.array):
        return scipy.signal.sosfiltfilt(low_filter, a)
    
    return _compute_signal_processing(ar, compute_lfp, out_freq, distrust_time)

def compute_bua(ar: xr.DataArray, bandpass_filter: BandpassFilter = BandpassFilter(low_freq=300, high_freq=6000), 
                lowpass_filter: LowpassFilter = LowpassFilter(freq=300), out_freq: float = 1000) -> xr.DataArray:
    
    distrust_time = max(lowpass_filter.get_distrust_time(), bandpass_filter.get_distrust_time())
    band_filter = bandpass_filter.get_sos_filter(ar["t"].attrs["fs"])
    low_filter = lowpass_filter.get_sos_filter(ar["t"].attrs["fs"])

    def compute_bua(a):
        bandpassed = scipy.signal.sosfiltfilt(band_filter, a)
        rectified = np.abs(bandpassed)
        lowpassed = scipy.signal.sosfiltfilt(low_filter, rectified)
        return lowpassed
    
    return _compute_signal_processing(ar, compute_bua, out_freq, distrust_time)

def compute_bandpass(ar: xr.DataArray, bandpass_filter: BandpassFilter = BandpassFilter(low_freq=300, high_freq=6000)) -> xr.DataArray:
    distrust_time = bandpass_filter.get_distrust_time()
    band_filter = bandpass_filter.get_sos_filter(ar["t"].attrs["fs"])
    def compute_filter(a):
        return scipy.signal.sosfiltfilt(band_filter, a)
    
    return _compute_signal_processing(ar, compute_filter, ar["t"].attrs["fs"], distrust_time)

def _compute_ffts_xr(
    da,
    time_dim="t",
    fs=1.0,
    window="hann",
    nperseg=256,
    noverlap=None,
    nfft=None,
    detrend="constant",
    return_onesided=True,
):
    if nfft is None:
        nfft = nperseg
    if noverlap is None:
        noverlap = nperseg // 2
    step = nperseg - noverlap

    win = xr.DataArray(scipy.signal.get_window(window, nperseg).astype(da.dtype), dims="segment")

    da_win = (
        da.rolling({time_dim: nperseg}, center=True)
        .construct("segment")
        .isel({time_dim: slice(nperseg, -nperseg, step)})
    )
    
    use_real = return_onesided and np.isrealobj(da_win)
    freqs = np.fft.rfftfreq(nfft, 1/fs) if use_real else np.fft.fftfreq(nfft, 1/fs)
    def handle_segment(seg, win):
        if detrend:
            seg = scipy.signal.detrend(seg, type=detrend, axis=-1)
        seg = seg*win
        if use_real:
            fft = np.fft.rfft(seg, n=nfft)
            if nfft % 2 == 0:  # even, has Nyquist
                fft[..., 1:-1] *= np.sqrt(2)
            else:  # odd, no Nyquist
                fft[..., 1:] *= np.sqrt(2)
        else:
            fft = np.fft.fft(seg, n=nfft)
        return fft
    
    res = xr.apply_ufunc(handle_segment, da_win, win, 
                         input_core_dims=[["segment"], ["segment"]], output_core_dims=[["f"]],
                         dask="parallelized",output_dtypes=[np.complex64], dask_gufunc_kwargs=dict(output_sizes=dict(f=freqs.size)))
    res["f"] = freqs

    return res, win

def compute_scaled_fft(
        signals: xr.DataArray, zscore: bool=True, 
        percent_overlap: float = 0.5, 
        scaling: Literal["density", "spectrum"] = "density", 
        approx_window_duration: float = 0.2, approx_freq_fs: float = 1.0, 
        detrend : Literal["linear", "constant"] = "linear",
        window="hann",
        time_dim: str = "t",
        return_onesided: bool = True
):
    fs = signals[time_dim].attrs["fs"]

    if zscore:
        signals = (signals - signals.mean(time_dim))/signals.std(time_dim)
    

    ffts, win = _compute_ffts_xr(signals, time_dim=time_dim, fs=fs, detrend=detrend,
                                 window=window, nperseg=int(approx_window_duration*fs), nfft=int(approx_freq_fs* fs),
                                 noverlap=int(approx_window_duration*fs*percent_overlap), return_onesided=return_onesided
    )
    scale = 1.0/(win**2).sum()
    if scaling=="density":
        scale/= fs 

    return ffts*np.sqrt(scale)



def compute_psd_from_scaled_fft(ffts: xr.DataArray, time_dim: str = "t", channel_dim="channel", channel_dim_suffix="_2") -> xr.DataArray:
    res = (ffts*np.conj(ffts.rename({channel_dim: channel_dim+channel_dim_suffix}))).mean(dim="t")
    try:
        if np.prod(res.data.chunksize) > 10**8:
            raise Exception(f"Huge chunk ! {res.data.chunksize} {ffts.data.chunksize}")
    except Exception:
        raise Exception(f"Unknown chunk !\n{res}\n{ffts}")
    return res

def compute_csd_from_scaled_fft(
    ffts: xr.DataArray,
    pairs: Union[
        Literal["all", "diag"],
        Callable[[xr.DataArray, xr.DataArray], xr.DataArray],
    ] = "all",
    result_type: Literal["matrix", "stacked", "drop"] = "stacked",
    keep_coords: Optional[List[Hashable]] = None,
    drop_coords: Optional[List[Hashable]] = None,
    time_dim: str = "t",
    channel_dim: str = "channel",
    channel1_rename: Union[str, Callable[[str], str]] = "_1",
    channel2_rename: Union[str, Callable[[str], str]] = "_2",
    channelpair_name: Union[str, Callable[[str], str]] = "_pair",
) -> xr.DataArray:
    """
    Compute cross-spectral density (CSD) from scaled FFTs.

    This function computes the CSD between all (or selected) channel pairs using
    scaled FFT data. Optionally, the set of pairs to compute can be filtered using
    a callable or predefined pattern. The core computation is conceptually:

        (ffts * np.conj(ffts.rename({channel_dim: channel_2}))).mean(dim=time_dim)

    Parameters
    ----------
    ffts : xr.DataArray
        FFT data with dimensions including `(time_dim, channel_dim, ...)`.
        Typically, each element represents the scaled FFT of a signal segment.
    pairs : {"all", "diag"} or callable, default "all"
        Specifies which channel pairs to compute.
        
        - `"all"`: compute all (channel_1, channel_2) combinations.
        - `"diag"`: compute only auto-spectra where channel_1 == channel_2.
        - Callable: function `(ch1, ch2) -> xr.DataArray[bool]` that returns
          a boolean mask of shape `(channel_1, channel_2)` indicating which
          pairs to include.
          
          Examples
          --------
          >>> lambda a1, a2: a1["structure"] == a2["structure"]
          >>> lambda a1, a2: (a1["chan_x"] - a2["chan_x"])**2 + (a1["chan_y"] - a2["chan_y"])**2 > 100
    result_type : {"matrix", "stacked"}, default "stacked"
        Determines the layout of the output data.
        
        - `"matrix"`: appends a new dimension named ``channel_dim + channel2_suffix``
          to represent channel pairs. The resulting array may contain NaNs if not
          all pairs are computed.
        - `"stacked"`: replaces the channel dimension with a single
          ``channelpair_name`` dimension containing both channel coordinates.
        - `"drop"`: similar to stacked, but removes any information about second channel.
          Particularly useful with diag.

          Examples
          --------
          >>> lambda a1, a2: a1["chan_num"] == a2["chan_num"] + 1
    keep_coords : list of hashable, optional
        Names of coordinates to keep in the output. If None, all coordinates
        are kept except those explicitly dropped.
    drop_coords : list of hashable, optional
        Names of coordinates to drop in the output. Useful when large amounts of
        metadata would otherwise be propagated.
    time_dim : str, default "t"
        Name of the time dimension in the input FFT data.
    channel_dim : str, default "channel"
        Name of the channel dimension in the input FFT data.
    channel1_rename, channel2_rename : str or callable, default "_1", "_2"
        Specifies how to rename the dimensions and coordinates for the two
        channel axes. If a string, it is used as a suffix.
    channelpair_name : str or callable, default "_pair"
        Name of the new channel-pair dimension when `result_type="stacked"`.
        If a string, it is used as a suffix.

    Returns
    -------
    csd : xr.DataArray
        Cross-spectral density data. The dimensionality depends on
        ``result_type``:
        
        - If `"matrix"`: has dimensions ``(..., channel_1, channel_2)``.
        - If `"stacked"`: has dimensions ``(..., channel_pair)``.

    Notes
    -----
    - Input FFTs should already be appropriately scaled (e.g., normalized by
      window power or segment count).
    - This function does not assume Hermitian symmetry.
    - Coordinate filtering (via ``keep_coords`` and ``drop_coords``) is applied
      after pair selection.

    Examples
    --------
    >>> csd = compute_csd_from_scaled_fft(
    ...     ffts,
    ...     pairs="diag",
    ...     result_type="matrix",
    ...     time_dim="t",
    ...     channel_dim="channel",
    ... )
    >>> csd.dims
    ('freq', 'channel_1', 'channel_2')
    """
    ffts = ffts.copy()
    # Helper function to apply rename suffix or callable
    def apply_rename(base_name: str, rename_spec: Union[str, Callable[[str], str]]) -> str:
        if callable(rename_spec):
            return rename_spec(base_name)
        else:
            return base_name + rename_spec
    
    channel1_dim = apply_rename(channel_dim, channel1_rename)
    channel2_dim = apply_rename(channel_dim, channel2_rename)
    channelpair_dim = apply_rename(channel_dim, channelpair_name)
    
    if not channel_dim in ffts.coords:
        ffts[channel_dim] = np.arange(ffts.sizes[channel_dim])
        created_chan_coord = True
    else:
        created_chan_coord = False


    ffts1 = ffts.rename({channel_dim: channel1_dim})
    ffts2 = ffts.rename({channel_dim: channel2_dim})

    if pairs == "all":
        mask = xr.ones_like(ffts1[channel1_dim], dtype=bool)*xr.ones_like(ffts2[channel2_dim], dtype=bool)
    elif pairs == "diag":
        mask = ffts1[channel1_dim] == ffts2[channel2_dim]
    else:
        mask = pairs(ffts1[channel_dim], ffts2[channel2_dim])
    mask = mask.drop_vars(mask.coords.keys())
    if keep_coords is None:
        rm_coords = []
    else:
        rm_coords = [c for c, v in ffts.coords.items() if not c in keep_coords]
    if drop_coords is not None:
        rm_coords +=list(drop_coords)
    rm_coords = [c for c in rm_coords if channel_dim in ffts[c].dims]
    ffts1 = ffts1.drop_vars(rm_coords)
    ffts2 = ffts2.drop_vars(rm_coords)
    rename_coords = [c for c in ffts1.coords if channel1_dim in ffts1[c].dims and c !=channel1_dim]
    ffts1 = ffts1.rename({k: apply_rename(k, channel1_rename) for k in rename_coords})
    ffts2 = ffts2.rename({k: apply_rename(k, channel2_rename) for k in rename_coords})
    
    if result_type == "matrix":
        #Can this be optimized ? Probably not natively in xarray, but maybe if we apply ufunc and directly manipulate dask arrays
        res = (ffts1 * np.conj(ffts2)).mean(time_dim).where(mask, drop=True) 
    elif result_type in ["stacked", "drop"]:
        mask["__chan_1_index"] =xr.DataArray(np.arange(mask.sizes[channel1_dim]), dims=channel1_dim)
        mask["__chan_2_index"] =xr.DataArray(np.arange(mask.sizes[channel2_dim]), dims=channel2_dim)
        if result_type=="drop":
            if (mask.sum(channel2_dim) > 1).any():
                raise Exception("Drop is not viable when you do not have a one to one correspondance...")
            ffts2=ffts2.drop_vars(ffts2.coords)
        mask = mask.stack({"_tmp": (channel1_dim, channel2_dim)}, create_index=False)
        mask = mask.where(mask, drop=True)
        mask=mask.rename(_tmp=channelpair_dim)
        ffts1 = ffts1.isel({channel1_dim:mask["__chan_1_index"]})
        ffts2 = ffts2.isel({channel2_dim:mask["__chan_2_index"]})
        res = (ffts1 * np.conj(ffts2)).mean(time_dim)
        res: xr.DataArray = res.drop_vars(["__chan_1_index", "__chan_2_index"])
    else:
        raise Exception("Wrong result_type option")
    if created_chan_coord:
        res = res.drop_vars([channel1_dim, channel2_dim], errors="ignore")
    return res
      
        
    

def compute_coh_from_psd(psd: xr.DataArray, channel_dim="channel", channel2_dim="channel_2") -> xr.Dataset:
    welch = compute_welch_from_psd(psd, channel_dim, channel2_dim)
    res = xr.Dataset()
    res["coherence"] = np.abs(psd)**2/(welch * welch.rename({channel_dim:channel2_dim}))
    res["coherence_phase"] = np.arctan2(np.imag(psd), np.real(psd))
    return res

# def compute_coh_from_psd2(psd: xr.DataArray, channel_dim="channel", pairs: List[T]) -> xr.Dataset:
#     welch = compute_welch_from_psd(psd, channel_dim, channel2_dim)
#     res = xr.Dataset()
#     res["coherence"] = np.abs(psd)**2/(welch * welch.rename({channel_dim:channel2_dim}))
#     res["coherence_phase"] = np.arctan2(np.imag(psd), np.real(psd))
#     return res

def compute_welch_from_psd(psd: xr.DataArray, channel_dim="channel", channel2_dim="channel_2") -> xr.DataArray:
    if (psd[channel_dim].to_numpy() != psd[channel2_dim].to_numpy()).any():
        raise Exception("Problem")
    welch = xr.apply_ufunc(lambda x: da.diagonal(x, axis1=-2, axis2=-1), psd, input_core_dims=[[channel_dim, channel2_dim]], output_core_dims=[[channel_dim]], dask="allowed")
    return np.real(welch)


