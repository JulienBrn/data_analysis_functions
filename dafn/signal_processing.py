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
    ar = ar.chunk(t=-1)
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
        signals: xr.DataArray, 
        zscore: bool=True, 
        percent_overlap: float = 0.5, 
        scaling: Literal["density", "spectrum"] = "density", 
        approx_window_duration: float = 1.0, approx_freq_fs: float = 1.0, 
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

def compute_coherence(a1: xr.DataArray, a2: xr.DataArray, time_dim="t"):
    # Compute cross- and auto-spectra (average over time/windows). Note, the magnitude is not squared
    sxy = (a1 * np.conj(a2)).mean(time_dim)
    sxx = (np.abs(a1) ** 2).mean(time_dim)
    syy = (np.abs(a2) ** 2).mean(time_dim)

    coherence = sxy / np.sqrt(sxx * syy)

    return coherence

def spiketimes_to_continuous(a: np.ndarray, out_fs: float = 1000, start: Optional[float] = None, end: Optional[float] = None, space: float = 1) -> xr.DataArray:
    if a.ndim != 1:
        raise Exception("Only 1 dimensional array is supported")
    if a.size == 0:
        raise Exception("No spike data")
    if start is None:
        start = a.min() - space
    if end is None:
        end = a.max() + space
    n_values = int((end - start)*out_fs) +1
    t = np.arange(n_values)/out_fs
    indices = np.rint((a-start)*out_fs).astype(int)
    indices = indices[(indices >= 0) & (indices < n_values)]
    ar = np.zeros(t.size, dtype=float)
    np.add.at(ar, indices, 1.0)
    neuron_continuous = xr.DataArray(ar, dims="t")
    neuron_continuous["t"] = start+t
    neuron_continuous["t"].attrs["fs"] = out_fs
    return neuron_continuous
