import xarray as xr, numpy as np, pandas as pd
from plotly.subplots import make_subplots
from plotly.express.colors import sample_colorscale
import plotly.graph_objects as go
from xhistogram.xarray import histogram
import functools

@functools.cache
def get_tabulator_content():
    import requests
    tabulator_content = f"""
    <style>
    {requests.get("https://unpkg.com/tabulator-tables@6.3.1/dist/css/tabulator.min.css").text}
    </style>
    <script>
    {requests.get("https://unpkg.com/tabulator-tables@6.3.1/dist/js/tabulator.min.js").text}
    </script>
    """
    return tabulator_content


def mk_plot(full_d: xr.Dataset, unit_id: int):
    unit_d = full_d.sel(unit=unit_id, unit2=unit_id, drop=True)
    unit_d = unit_d.assign_coords(unit=unit_id)
    if "template_similarity" in unit_d:
        unit_d = unit_d.drop_vars(["template_similarity"])
    unit_d = unit_d.sel(sparse_channel=(unit_d["sparse_channel_name"] != ''))
    unit_d["templates"] = unit_d["templates"].sel(channel=unit_d["sparse_channel_name"])
    unit_d["sparse_channel"] = unit_d["sparse_channel_name"]
    unit_d = unit_d.sel(rnd_spike=(unit_d["rnd_spike_unit"]==unit_id))
    unit_d = unit_d.sel(spike=(unit_d["spike_unit"]==unit_id))
    unit_d["active_unit"] = xr.DataArray(full_d.sel(unit=(full_d["sparse_channel_name"] == unit_d["primary_channel"].item()).any("sparse_channel"))["unit"].to_numpy(), dims="active_unit")
    
    traces = {}
    zooms= {}

    n_active_units = (unit_d["active_unit"] != unit_d["unit"].item()).sum().item()
    unit_index_v = (unit_d["active_unit"] != unit_d["unit"].item()).cumsum("active_unit")
    unit_index_d= {k.item():unit_index_v.sel(active_unit=k.item()).item() for k in unit_d["active_unit"]}

    def get_unit_color(unit):
        if hasattr(unit, "item"):
            unit = unit.item()
        if unit == unit_d["unit"].item():
            return "red"
        elif unit in unit_d["active_unit"]:
            return sample_colorscale('viridis', [unit_index_d[unit]/n_active_units])[0]
        else:
            return "grey"
    def get_unit_size(unit):
        if hasattr(unit, "item"):
            unit = unit.item()
        if unit == unit_d["unit"].item():
            return 10
        elif unit in unit_d["active_unit"]:
            return 7
        else:
            return 5
        
    n_active_channels = (unit_d["sparse_channel"] != unit_d["primary_channel"].item()).sum().item()
    channel_index_v = (unit_d["sparse_channel"] != unit_d["primary_channel"].item()).cumsum("sparse_channel")
    channel_index_d= {k.item():channel_index_v.sel(sparse_channel=k.item()).item() for k in unit_d["sparse_channel"]}
    def get_channel_color(channel):
        if hasattr(channel, "item"):
            channel = channel.item()
        if channel == unit_d["primary_channel"].item():
            return "black"
        elif channel in unit_d["sparse_channel"]:
            # return sample_colorscale('Aggrnyl', [channel_index_d[channel]/n_active_channels])[0]
            return "green"
        else:
            return "grey"
    
    x_c = full_d["channel_loc"].sel(channel=unit_d["primary_channel"], space_ax="x")
    x_diff = full_d["channel_loc"].sel(channel=unit_d["sparse_channel"], space_ax="x") - x_c
    x_r = np.abs(x_diff).max()

    y_c = full_d["channel_loc"].sel(channel=unit_d["primary_channel"], space_ax="y")
    y_diff = full_d["channel_loc"].sel(channel=unit_d["sparse_channel"], space_ax="y") - y_c
    y_r = np.abs(y_diff).max()

    zooms = zooms | dict(probe_display=dict(x=[x_c-x_r*1.2, x_c+x_r*1.2], y=[y_c-y_r*1.2, y_c+y_r*1.2]))

    channel_trace = go.Scatter(
        x = unit_d["channel_loc"].sel(space_ax="x"), y = unit_d["channel_loc"].sel(space_ax="y"), 
        customdata=unit_d[["channel"]].to_dataframe().reset_index(), hovertemplate="%{customdata[0]}<br>x: %{x}<br>y: %{y}",
        marker_line_color=xr.apply_ufunc(get_channel_color, unit_d["channel"], vectorize=True), 
        mode='markers', marker_symbol="square", marker_color="white", marker_line_width=1, opacity=0.5,
    )

    unit_locations = go.Scatter(
        x =full_d["unit_locations"].sel(space_ax="x"), y = full_d["unit_locations"].sel(space_ax="y"), 
        customdata=full_d[["unit"]].to_dataframe().reset_index(), hovertemplate="unit: %{customdata[0]}<br>x: %{x}<br>y: %{y}",
        marker_color=xr.apply_ufunc(get_unit_color, full_d["unit"], vectorize=True), 
        marker_size= xr.apply_ufunc(get_unit_size, full_d["unit"], vectorize=True),
        mode="markers", opacity=0.5, marker_symbol="cross", marker_line_width=1,                       
    )
    traces= traces | dict(probe_display=[channel_trace, unit_locations])

    primary_waveform_density = unit_d[["template_density", "templates"]].sel(sparse_channel=unit_d["primary_channel"])
    primary_waveform_density_trace = go.Heatmap(x=primary_waveform_density["wf_t"], y=primary_waveform_density["waveform_amplitude_bin"], 
                                        z=primary_waveform_density["template_density"].transpose("wf_amp_bin", "wf_t"), 
                                        showscale=False)
    primary_waveform_template_trace = go.Scatter(x=primary_waveform_density["wf_t"], 
                                        y=primary_waveform_density["templates"], line_color=get_channel_color(unit_d["primary_channel"]))
    traces= traces | dict(primary_waveform=[primary_waveform_density_trace, primary_waveform_template_trace])




    x_resize_max = np.diff(np.sort(unit_d["channel_loc"].sel(channel=unit_d["sparse_channel"], space_ax="x").to_numpy())).max() / (unit_d["wf_t"].max() - unit_d["wf_t"].min()).item()
    y_resize_max = np.diff(np.sort(unit_d["channel_loc"].sel(channel=unit_d["sparse_channel"], space_ax="y").to_numpy())).max() / (unit_d["waveform_amplitude_bin"].max() - unit_d["waveform_amplitude_bin"].min()).item()
    x_resize = x_resize_max*0.8
    y_resize=y_resize_max*0.8

    def transform_x(a, chan):
        return a*x_resize + unit_d["channel_loc"].sel(channel=chan,space_ax="x").item()
    def transform_y(a, chan):
        return a*y_resize + unit_d["channel_loc"].sel(channel=chan, space_ax="y").item()

    waveform_template_traces = []
    waveform_density_traces = []

    for chan in unit_d["sparse_channel"]:
        df = unit_d["template_density"].sel(sparse_channel=chan).to_dataframe().reset_index()
        waveform_density_traces.append(go.Heatmap(
            x=transform_x(df["wf_t"], chan), y=transform_y(df["waveform_amplitude_bin"], chan), 
            z=df["template_density"], 
            customdata=df[["wf_t", "waveform_amplitude_bin"]], 
            hovertemplate=("channel: "+ str(chan.item()) + "<br>t: %{customdata[0]}<br>amp: %{customdata[1]}<br>count: %{z}"),
            showscale=False))
        waveform_template_traces.append(go.Scatter(
            x=transform_x(unit_d["wf_t"], chan), y=transform_y(unit_d["templates"].sel(sparse_channel=chan), chan), 
            line_color=get_channel_color(chan), mode="lines",
            customdata=unit_d.sel(sparse_channel=chan)[["templates"]].to_dataframe().reset_index()[["wf_t", "templates"]], 
            hovertemplate=("channel: "+ str(chan.item()) + "<br>t: %{customdata[0]}<br>amp: %{customdata[1]}"),
        ))

    current_unit_locations = go.Scatter(x =[unit_d["unit_locations"].sel(space_ax="x").item()], y = [unit_d["unit_locations"].sel(space_ax="y").item()], 
                            mode="markers", opacity=0.5, marker_symbol="cross", marker_line_width=1, marker_color=get_unit_color(unit_d["unit"])
    )

    traces= traces | dict(channel_traces=waveform_density_traces + waveform_template_traces + [current_unit_locations])

    correlogram_trace = go.Bar(x=unit_d["corr_t"], y=unit_d["correlogram"])
    traces= traces | dict(correlogram_trace=[correlogram_trace])

    zoom = unit_d["spike_amplitudes"].quantile([0.01, 0.99]).to_numpy()
    spike_density = go.Heatmap(x=unit_d["spike_time_bin"], y=unit_d["spike_amplitude_bin"], 
                                        z=unit_d["spike_density_map"].transpose("spike_amp_bin", "spike_time_bin"), 
                                        showscale=False)
    mean_spike_amp = go.Scatter(x=unit_d["spike_time_bin"], y=unit_d["spike_mean_amp"], mode="lines", line_color="black")
    traces= traces | dict(spike_density=[spike_density, mean_spike_amp])
    zooms = zooms | dict(spike_density=dict(y=[zoom[0], zoom[1]]))

    units_per_sec = full_d["spike_density_map"].sel(unit=unit_d["active_unit"]).sum("spike_amp_bin")/30
    units_per_sec_plots = []
    for unit in unit_d["active_unit"]:
        units_per_sec_plots.append(go.Scatter(x=units_per_sec["spike_time_bin"], y=units_per_sec.sel(active_unit=unit), 
                                            hovertemplate=("unit: "+ str(unit.item()) + "<br>t: %{x}<br>fs: %{y}"),
                                            line_color=get_unit_color(unit), opacity =0.8 if unit==unit_d["unit"] else 0.2,
                                            mode="lines"))
    traces= traces | dict(units_per_sec_plots=units_per_sec_plots)

    spike_amp_count = full_d["spike_density_map"].sel(unit=unit_d["unit"]).sum("spike_time_bin")

    spike_amp_trace = go.Bar(x=spike_amp_count["spike_amplitude_bin"], y=spike_amp_count)
    traces= traces | dict(spike_amp_trace=[spike_amp_trace])

    isi_trace = go.Bar(x=unit_d["isi_t"], y=unit_d["isi_hist"])
    traces= traces | dict(isi_trace=[isi_trace])

    for i in range(2):
        raw_trace_data = unit_d.isel(raw_seg=i)
        start = raw_trace_data["raw_trace_t"].min().item()
        end = raw_trace_data["raw_trace_t"].max().item()
        raw_trace = go.Scatter(x=raw_trace_data["raw_trace_t"], y=raw_trace_data["raw_trace"], mode="lines", line_color=get_channel_color(unit_d["primary_channel"]))
        time_cond = (full_d["spike_time"] >= start) & (full_d["spike_time"] <= end)
        unit_cond = full_d["spike_unit"].isin(unit_d["active_unit"])
        spike_ar = full_d[["spike_time", "spike_amplitudes", "spike_unit"]].sel(spike=time_cond & unit_cond)
        avg_amp = raw_trace_data["raw_trace"].mean().item()
        spike_trace = go.Scatter(x=spike_ar["spike_time"], y=spike_ar["spike_time"]*0+avg_amp, customdata=spike_ar[["spike_unit"]].to_dataframe().reset_index()[["spike_unit"]],
                            marker_color=xr.apply_ufunc(get_unit_color, spike_ar["spike_unit"], vectorize=True), marker_size=20*xr.apply_ufunc(lambda u:2, spike_ar["spike_unit"], vectorize=True),
                            mode="markers", opacity=0.8, marker_symbol="line-ns", marker_line_width=2, marker_line_color=xr.apply_ufunc(get_unit_color, spike_ar["spike_unit"], vectorize=True),
                            hovertemplate="unit: %{customdata[0]}<br>t: %{x}"
        )
        freqs = histogram(unit_d["spike_time"], bins=np.arange(start, end+0.1, 0.1))
        zoom = freqs.idxmax("spike_time_bin").item()
        traces= traces | {f"raw_seg_{i}": [raw_trace, spike_trace]}
        zooms = zooms | {f"raw_seg_{i}":dict(x=[zoom-0.05, zoom+0.05])}
    
    fig = make_subplots(rows=4, cols=5, column_widths=[0.3, 0.2, 0.17, 0.16, 0.17], row_heights=[0.4, 0.15, 0.15, 0.3], vertical_spacing=0.1,
                    specs=[[{}, {"rowspan":3}, {}, {}, {}],[{"rowspan":2}, None, {"colspan": 3}, None, None], [None, None, {"colspan": 3}, None, None], [{"colspan":2}, None, {"colspan": 3}, None, None]],
                    subplot_titles = ['Probe', 'Unit waveform across channels', 'Unit auto correlation', 
                                    "Unit ISI distribution", "Amplitude distribution", 'Unit waveform on primary channel', "Unit amplitude", "Unit firing rate", "Raw trace slice1", "Raw trace slice2"])
    
    fig.add_traces(traces["probe_display"], rows=1, cols=1)
    fig.add_traces(traces["channel_traces"], rows=1, cols=2)
    fig.add_traces(traces["primary_waveform"], rows=2, cols=1)
    fig.add_traces(traces["correlogram_trace"], rows=1, cols=3)
    fig.add_traces(traces["isi_trace"], rows=1, cols=4)
    fig.add_traces(traces["spike_amp_trace"], rows=1, cols=5)
    fig.add_traces(traces["spike_density"], rows=2, cols=3)
    fig.add_traces(traces["units_per_sec_plots"], rows=3, cols=3)
    fig.add_traces(traces["raw_seg_0"], rows=4, cols=1)
    fig.add_traces(traces["raw_seg_1"], rows=4, cols=3)
    fig.update_yaxes(range=zooms["spike_density"]["y"], row=2, col=3)
    fig.update_yaxes(range=zooms["probe_display"]["y"], row=1, col=1)
    fig.update_xaxes(range=zooms["probe_display"]["x"], row=1, col=1)
    fig.update_xaxes(range=zooms["raw_seg_0"]["x"], row=4, col=1)
    fig.update_xaxes(range=zooms["raw_seg_1"]["x"], row=4, col=3)
    fig.update_layout(showlegend=False,  title=dict(text=f"Information for unit {unit_id} on channel {unit_d['primary_channel'].item()}", xanchor='center', x=0.5),)
    config = {'scrollZoom': True, 'displaylogo': False, 'toImageButtonOptions': {
        'format': 'svg', # one of png, svg, jpeg, webp
        'filename': f'summary_unit_{unit_d["primary_channel"].item()}',
    }}

    fig_html = fig.to_html(config=config, default_height="90%")

    # with (tmp_result_path/f"fig_only_{unit_id}.html").open("w") as f:
        # f.write(fig_html)

    qm = full_d.sel(unit=unit_d["active_unit"])["quality_metrics"].to_dataframe().reset_index().rename(columns=dict(quality_metric="metric", quality_metrics="value"))
    tm = full_d.sel(unit=unit_d["active_unit"])["template_metrics"].to_dataframe().reset_index().rename(columns=dict(template_metric="metric", template_metrics="value"))
    all = pd.concat([qm, tm], ignore_index=True)
    other = all.set_index(["metric", "unit"]).unstack("metric")["value"].reset_index()



    # autoColumnsDefinitions:[
    #         {field: "metric", headerFilter:true}, 
    #         {field: "unit", headerFilter:true},
    #     ],
    div = f"""
    <div>
        <button type="button" id="view_metrics">
        view_metrics
        </button>
    </div>
    <div style="overflow-x:scroll;max-width: 100vw;" id="table_div">
    <div id="example-table"></div>
    </div>
    """
    table_html = f"""
    <script>
    var tabledata = {list(other.fillna("None").to_dict(orient="index").values())}
    unit = {unit_id}
    """+ """
    var table = new Tabulator("#example-table", {
        data:tabledata,          
        layout:"fitDataTable",      
        autoColumns:true,
        columnDefaults:{
            headerFilter:true, 
        },
        rowFormatter:function(row){
            //row - row component
            
            var data = row.getData();
            
            if(data.unit == unit){
                row.getElement().style.backgroundColor = "yellow";
            }
        },
        
    });
    </script>
    """ + """
    <script>
    var mtable = document.getElementById("table_div");
    btn = document.getElementById('view_metrics');
    mtable.style.display = "none";
    btn.innerText="view metrics";
    function toggle() {
    if (mtable.style.display == "none") {
        mtable.style.display = "block";
        btn.innerText="hide metrics";
    }
    else {
        mtable.style.display = "none";
        btn.innerText="view metrics";
    }
    }
    btn.addEventListener('click', toggle);
    </script>
    """
    return get_tabulator_content()+div+fig_html+table_html