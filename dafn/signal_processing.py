import xarray as xr, pandas as pd, numpy as np
from pydantic import BaseModel
import scipy.signal, re
from abc import ABC, abstractmethod
from typing import Literal
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
    return (ffts*np.conj(ffts.rename({channel_dim: channel_dim+channel_dim_suffix}))).mean(dim="t")
    

def compute_coh_from_psd(psd: xr.DataArray, channel_dim="channel", channel2_dim="channel_2") -> xr.Dataset:
    welch = compute_welch_from_psd(psd, channel_dim, channel2_dim)
    res = xr.Dataset()
    res["coherence"] = np.abs(psd)**2/(welch * welch.rename({channel_dim:channel2_dim}))
    res["coherence_phase"] = np.arctan2(np.imag(psd), np.real(psd))
    return res

def compute_welch_from_psd(psd: xr.DataArray, channel_dim="channel", channel2_dim="channel_2") -> xr.DataArray:
    if (psd[channel_dim].to_numpy() != psd[channel2_dim].to_numpy()).any():
        raise Exception("Problem")
    welch = xr.apply_ufunc(lambda x: da.diagonal(x, axis1=-2, axis2=-1), psd, input_core_dims=[[channel_dim, channel2_dim]], output_core_dims=[[channel_dim]], dask="allowed")
    return np.real(welch)


