import spikeinterface as si
import spikeinterface.preprocessing as sip
import spikeinterface.sorters as sis
import os, tempfile, shutil, yaml
from pathlib import Path
import spikeinterface as si
import yaml
import numpy as np, xarray as xr, pandas as pd
import tqdm.auto as tqdm
from xhistogram.xarray import histogram

def preprocess(rec : si.BaseRecording, start=None, end=None, low_freq=300, high_freq=6000, num_random_chunks=100):
    if rec.get_num_segments() != 1:
            raise Exception("Only single segment recordings supported")
    if start or end:
        rec : si.BaseRecording = rec.frame_slice(
            int(start * rec.sampling_frequency) if start else None, 
            int(end* rec.sampling_frequency) if end else None
        )
    rec: si.BaseRecording = sip.phase_shift(rec)
    rec = sip.bandpass_filter(rec, freq_max=high_freq, freq_min=low_freq)
    bad_channel_ids, _= sip.detect_bad_channels(rec, chunk_duration_s= 0.5, method= "coherence+psd", num_random_chunks= num_random_chunks)
    rec = rec.remove_channels(bad_channel_ids)
    rec = sip.common_reference(rec, local_radius=[50, 100], reference= "local")
    return rec, bad_channel_ids

def spikesort(rec: si.BaseRecording, sorter_name: str = "kilosort4", tmp_folder_path = None, sorter_params = None):
    if sorter_params is None:
         sorter_params = {}
    if tmp_folder_path is None:
        created=True
        home = os.path.expanduser("~")
        tmp_folder_path = Path(tempfile.mkdtemp(dir=home))
    else:
        created=False
    if sorter_name == "kilosort4":
        base_sorter_params = dict(do_CAR= False, do_correction= False, skip_kilosort_preprocessing= False)
    else:
        base_sorter_params = {}
    if shutil.disk_usage(tmp_folder_path.parent).free < 1.5*rec.get_memory_size():
        raise Exception(f"They may be not enough free space at location {tmp_folder_path.parent} (we take factor 1.5 margin). Free space: {shutil.disk_usage(tmp_folder_path.parent).free}. Expected size: {rec.get_memory_size()}.")
    try:
        result = sis.run_sorter(recording = rec, folder= tmp_folder_path,  sorter_name=sorter_name, **(base_sorter_params | sorter_params))
    except Exception:
         print(f"Problem running sorter, temp dir is {tmp_folder_path}. You can inspect results there.")
         raise
    else: 
        if created and tmp_folder_path.exists():
            shutil.rmtree(tmp_folder_path)
    return result


default_analyzer_params = yaml.safe_load(
    """
    random_spikes:
        method: "uniform"
        max_spikes_per_unit: 500
    waveforms:
        ms_before: 1
        ms_after: 2
    amplitude_scalings: False
    spike_locations: False
    """
)

def create_spikeinterface_analyzer(rec: si.BaseRecording, sorting : si.BaseSorting, params, job_kwargs):
    analyzer : si.SortingAnalyzer = si.create_sorting_analyzer(sorting, rec)
    real_params = {k:(analyzer.get_default_extension_params(k) | params.get(k, {}))  
            for k in analyzer.get_computable_extensions() if params.get(k, {}) != False}
    analyzer.compute_several_extensions(real_params, **job_kwargs)


dimensions = dict(principal_components=["rnd_spike", "pc", "sparse_channel"],
                noise_levals=["channel"],
                random_spikes=["rnd_spike"],
                unit_locations=["unit", "space_ax"],
                spike_amplitudes=["spike"],
                templates=["unit", "wf_t", "channel"],
                waveforms=["rnd_spike", "wf_t", "sparse_channel"],
                #   spike_locations=["spike"],
                template_metrics=["unit", "template_metric"],
                template_similarity=["unit", "unit2"],
                quality_metrics=["unit", "quality_metric"],
)

def spike_interface_to_xarray(analyzer: si.SortingAnalyzer, sorting: si.BaseSorting, recording: si.BaseRecording):
   extensions = analyzer.get_loaded_extension_names()
   d = xr.Dataset()
   ssv = sorting.to_spike_vector()
   d["spike_time"] = xr.DataArray(ssv["sample_index"]/analyzer.sampling_frequency, dims="spike")
   d["spike_unit_index"] = xr.DataArray(ssv["unit_index"], dims="spike")
   
   for ext in extensions:
      s = analyzer.get_extension(ext)
      data = s.get_data()
      if ext in dimensions:
            if ext in ["template_metrics", "quality_metrics"]:
                data = data.astype(float)
            d[ext] = xr.DataArray(data, dims=dimensions[ext])
   if "correlograms" in extensions:
      corr, corr_bins = analyzer.get_extension("correlograms").get_data()
      d["correlogram"] = xr.DataArray(corr, dims=["unit", "unit2", "corr_t"])
      d["corr_t"] = (corr_bins[1:] + corr_bins[: -1])/2000
   if "isi_histograms" in extensions:
      isi, isi_bins = analyzer.get_extension("isi_histograms").get_data()
      d["isi_hist"] = xr.DataArray(isi, dims=["unit", "isi_t"])
      d["isi_t"] = (isi_bins[1:] + isi_bins[: -1])/2000
   def get_sparsity(unit):
      ids = analyzer.sparsity.unit_id_to_channel_ids[unit.item()]
      return np.pad(ids, mode="constant", constant_values="", pad_width=(0, d["sparse_channel"].size - len(ids)))
   d["sparse_channel_name"] = xr.DataArray([get_sparsity(unit) for unit in d["unit"]], dims=["unit", "sparse_channel"])
   d = d.sel(sparse_channel=(d["sparse_channel_name"]!="").any("unit"))
   d["space_ax"] = ["x", "y", "z"][:d["space_ax"].size]
   d = xr.merge([d, xr.DataArray(recording.get_channel_locations(axes="xy"), dims=["channel", "space_ax"], name="channel_loc", coords=dict(space_ax=["x", "y"]))])
   d["channel"] = recording.channel_ids
   d["rnd_spike_unit"] = xr.DataArray(analyzer.get_extension("random_spikes").get_random_spikes()["unit_index"], dims="rnd_spike")
   d["wf_t"] = (np.arange(d.sizes["wf_t"]) - analyzer.get_extension("waveforms").nbefore)/recording.sampling_frequency
   d["rnd_spike"] = d["random_spikes"]
   d = d.drop_vars("random_spikes")
   d["spike_unit"] = d["unit"].isel(unit=d["spike_unit_index"])
   d = d.set_coords(["rnd_spike_unit", "spike_unit", "spike_unit_index", "sparse_channel_name"])
   is_channel_in_sparse = (d["channel"] == d["sparse_channel_name"]).any("sparse_channel")
   if (is_channel_in_sparse.astype(int).sum("channel") < 1).any():
       raise Exception(f'Some units have no sparse channels...\n{(is_channel_in_sparse.astype(int).sum("channel"))}')
   d["primary_channel"] = ((d["channel_loc"] - d["unit_locations"])**2).sum("space_ax").where(is_channel_in_sparse).idxmin("channel")
   if d["primary_channel"].isnull().any():
       raise Exception("na primary channels...")
   d["unit2"] = d["unit"].to_numpy()
   return d

def compute_spike_additional_info(d: xr.Dataset, t_bins, n_amp_bins=50) -> xr.Dataset:
   progress= tqdm.tqdm(desc="spike_amp_density", total=d.sizes["unit"])
   def compute_spike_density_map(g):
      r = histogram(g["spike_time"], g["spike_amplitudes"], bins=[t_bins, n_amp_bins], density=True)
      r["spike_amplitude_bin"] = xr.DataArray(r["spike_amplitudes_bin"].to_numpy(), dims="spike_amplitudes_bin")
      r["spike_amplitudes_bin"] = np.arange(n_amp_bins)
      r = r.rename(spike_amplitudes_bin="spike_amp_bin")
      progress.update()
      return r
   
   ret = xr.Dataset()
   ret["spike_density_map"] = d[["spike_time", "spike_amplitudes"]].groupby("spike_unit").apply(compute_spike_density_map).rename(spike_unit="unit")
   ret["spike_mean_amp"] = d.set_coords("spike_time")["spike_amplitudes"].groupby(spike_unit=xr.groupers.UniqueGrouper(), spike_time=xr.groupers.BinGrouper(t_bins)
               ).mean().drop_vars("spike_time_bins").rename(spike_time_bins="spike_time_bin", spike_unit="unit")
   return ret

def compute_template_densities(d: xr.Dataset, n_amp_wf_bins) -> xr.DataArray:
   progress= tqdm.tqdm(desc="template_density_map", total=d.sizes["unit"])
   def compute_template_density_map(g):
      r = histogram(g["waveforms"],  dim=["rnd_spike"], bins=n_amp_wf_bins)
      r["waveform_amplitude_bin"] = xr.DataArray(r["waveforms_bin"].to_numpy(), dims="waveforms_bin")
      r["waveforms_bin"] = np.arange(101)
      r = r.rename(waveforms_bin="wf_amp_bin")
      progress.update()
      return r
   return d[["waveforms"]].groupby("rnd_spike_unit").apply(compute_template_density_map).rename(rnd_spike_unit="unit")

def compute_recording_samples(d: xr.Dataset, recording: si.BaseRecording, segment_duration: float, n_segments):
    min_t = d["spike_time"].min().item()
    max_t = d["spike_time"].max().item()
    t_bins = np.arange(min_t, max_t, segment_duration)
    spike_count_density_grp =  d[["spike_time"]].groupby(spike_unit=xr.groupers.UniqueGrouper(), spike_time=xr.groupers.BinGrouper(t_bins))
    spike_count_density = spike_count_density_grp.count().drop_vars("spike_time_bins").rename(spike_time_bins="spike_time_bin", spike_unit="unit")["spike_time"].fillna(0)
    sorted = xr.apply_ufunc(np.argsort, spike_count_density, input_core_dims=[["spike_time_bin"]], output_core_dims=[["spike_time_bin"]], kwargs=dict(axis=-1))
    selected = sorted.isel(spike_time_bin=slice(-n_segments, None)).rename(spike_time_bin="raw_seg")
    progress = tqdm.tqdm(total=selected.size, desc="extracting some segments")
    def get_trace(position, unit):
        pc = d["primary_channel"].sel(unit=unit).item()
        if pd.isna(pc):
            print(d["primary_channel"])
            print(d["primary_channel"].isnull())
            raise Exception(f"nan primary channel for unit {unit}")
        start_frame = int((t_bins[position]) *recording.sampling_frequency)
        end_frame = start_frame+int(segment_duration*recording.sampling_frequency)
        try:
            trace = recording.get_traces(channel_ids=[pc], start_frame=start_frame, end_frame=end_frame)[:, 0]
        except Exception:
            print(f"pc={pc}\n{d['primary_channel']}")
            raise
        coords = (np.arange(start_frame, end_frame)/recording.sampling_frequency)
        if trace.shape != coords.shape:
            trace=np.pad(trace, (0, coords.size-trace.size), constant_values=np.nan)
        progress.update()
        return trace, coords
    return xr.apply_ufunc(get_trace, selected, selected["unit"], output_core_dims=[["rec_t_index"], ["rec_t_index"]], vectorize=True)


def xarray_sorting_analysis_data(analyzer: si.SortingAnalyzer, sorting: si.BaseSorting, recording: si.BaseRecording, t_bin_secs, n_amp_bins, n_amp_wf_bins):
   d = spike_interface_to_xarray(analyzer, sorting, recording)
   max_t = recording.get_duration()
   t_bins = np.array(list(range(0, int(max_t), t_bin_secs)) + [max_t])
   d = xr.merge([d, compute_spike_additional_info(d, t_bins, n_amp_bins)])
   d["template_density"] = compute_template_densities(d, n_amp_wf_bins)
   d["raw_trace"], d["raw_trace_t"] = compute_recording_samples(d, recording, 1, 2)
   d = d.set_coords("raw_trace_t")
   return d

