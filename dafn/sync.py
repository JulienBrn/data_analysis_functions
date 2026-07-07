import typing
from typing import Literal

import numpy as np
import pandas as pd
import plotly.colors as pc
import plotly.express as px
import plotly.graph_objects as go
import tqdm.auto as tqdm
import xarray as xr
from plotly.subplots import make_subplots


def get_best_shift(diffs: np.ndarray, tol: float):
    if diffs.size == 0:
        raise Exception("No data")
    diff_min = diffs - tol
    diff_max = diffs + tol
    merged = np.concatenate((diff_min, diff_max))
    order = merged.argsort()
    sorted_merged = merged[order]
    counts = np.cumsum(2 * (order < diff_min.size) - 1)
    best_match = counts.argmax()
    n_matches = counts[best_match]
    min_shift = sorted_merged[best_match]
    max_shift = sorted_merged[best_match + 1]
    if np.isnan(min_shift + max_shift):
        raise Exception("GOT NAN")
    return n_matches, min_shift, max_shift


class SlopeSearch(typing.NamedTuple):
    min: float
    max: float
    n_branches: int
    precision: float

    @property
    def n_iters(self) -> int:
        if self.max - self.min < self.precision:
            return 1
        return self.n_branches * int(np.floor(np.log2((self.max - self.min) / self.precision) / np.log2(self.n_branches)) + 1)

    def optimize(self, func):
        ncalls = 0
        test_values = []
        res_values = []
        curr = (self.max + self.min) / 2
        precision = self.max - curr
        if precision < self.precision:
            curr_val = func(curr)[1]
            ncalls += 1
        while precision > self.precision:
            test_space = np.linspace(curr - precision, curr + precision, self.n_branches, endpoint=False) + precision / self.n_branches
            vals = [func(x) for x in test_space]
            test_values += list(test_space)
            res_values += [v[0] for v in vals]
            best = np.argmax([v[0] for v in vals])
            curr_val = vals[best][1]
            curr = test_space[best]
            precision = precision / self.n_branches
            ncalls += self.n_branches
        if ncalls != self.n_iters:
            print(f"n_iter error, got {ncalls} calls, expected {self.n_iters}")
        return curr, curr_val


class TolSearch(typing.NamedTuple):
    min: float
    max: float
    precision: float
    percentage: float = 0.9

    @property
    def n_iters(self) -> int:
        return 1 + int(np.floor(np.log2((self.max - self.min) / self.precision)) + 1)

    def optimize(self, func):
        min_tol = self.min
        max_tol = self.max
        max_val, state = func(max_tol)
        min_val = 0
        while max_tol - min_tol > self.precision:
            curr = (max_tol + min_tol) / 2
            val, s = func(curr)
            if val > self.percentage * (max_val - min_val):
                max_tol = curr
                max_val, state = val, s
            else:
                min_val = val
                min_tol = curr
        return (max_tol + min_tol) / 2, state


def compute_initial_sync_values(
    ref_arrs: list[np.ndarray],
    rel_arrs: list[np.ndarray],
    tol_search: TolSearch = TolSearch(0, 0.050, 10**-3, 0.95),
    slope_search: SlopeSearch = SlopeSearch(0.999, 1.001, 20, 10**-6),
    progress: tqdm.tqdm | None = None,
) -> tuple[int, float, float, float]:
    tol_search = TolSearch(*tol_search)
    slope_search = SlopeSearch(*slope_search)
    if len(ref_arrs) != len(rel_arrs):
        raise Exception("Expected same number of categories")
    if progress is None:
        progress = tqdm.tqdm(desc="Computing initial sync values", total=tol_search.n_iters * slope_search.n_iters)
    with progress:
        computed_diff_arrs = {}

        def get_diff_arr(slope: float):
            rng = np.random.default_rng()
            if slope not in computed_diff_arrs:
                marr = [(ref_arrs[group][~np.isnan(ref_arrs[group])], rel_arrs[group][~np.isnan(rel_arrs[group])]) for group in range(len(ref_arrs))]
                total_size = sum([ref.size * rel.size for ref, rel in marr])
                if total_size > 3 * 10**5:
                    factor = np.sqrt(total_size / (3 * 10**5))
                    marr = [(rng.choice(ref, int(ref.size / factor)), rng.choice(rel, int(rel.size / factor))) for ref, rel in marr]
                res = np.concatenate([(ref.reshape(-1, 1) - slope * rel.reshape(1, -1)).flatten() for ref, rel in marr])
                computed_diff_arrs[slope] = res
            return computed_diff_arrs[slope]

        def evaluate_from_slope_and_shift(tol, slope):
            progress.update()
            progress.set_postfix(tol=tol, slope=slope)
            diffs = get_diff_arr(slope)
            n, min_shift, max_shift = get_best_shift(diffs, tol)
            return n, (n, min_shift, max_shift)

        def evaluate_from_tol(tol):
            slope, (n, min_shift, max_shift) = slope_search.optimize(lambda slope: evaluate_from_slope_and_shift(tol, slope))
            return n, (n, slope, min_shift, max_shift)

        tol, (n, slope, min_shift, max_shift) = tol_search.optimize(evaluate_from_tol)
    return n, tol, slope, min_shift, max_shift


def compute_match(ref_ar, rel_ar, tol, cost_func, progress):
    initial_cost = (0, 0)
    cache = {}
    stack = [(0, 0, False)]

    while stack:
        i, j, expanded = stack.pop()

        if (i, j) in cache:
            continue

        if expanded is not False:
            progress.update()
            results = []
            for (ci, cj), (incnb, incdiff, added) in expanded:
                (nb, diff), matching = cache[(ci, cj)]
                result = ((nb + incnb, diff + incdiff), added + matching)
                results.append(result)
            try:
                cache[(i, j)] = min(results, key=lambda r: cost_func(*r[0]))
            except:
                print(i, j, len(ref_ar), len(rel_ar))
                raise
        else:
            cases = []
            if i < ref_ar.size and j < rel_ar.size:
                ta = ref_ar[i]
                tb = rel_ar[j]
                diff = abs(ta - tb)
                if diff < tol:
                    cases.append(((i + 1, j + 1), (1, diff, [(i, j)])))
                if tb - ta < tol:
                    cases.append(((i, j + 1), (0, 0, [])))
                if ta - tb < tol:
                    cases.append(((i + 1, j), (0, 0, [])))
                stack.append((i, j, cases))
                for ci, _ in cases:
                    stack.append((ci[0], ci[1], False))
            else:
                cache[(i, j)] = initial_cost, []

    return cache[(0, 0)]


def compute_match_intervals(
    ref_ar_start,
    rel_ar_start,
    ref_ar_end,
    rel_ar_end,
    tol,
    cost_func=lambda n, diff: diff - n,
    interval_func=lambda n_ref, n_rel, ref_hole_dur, rel_hole_dur, ref_dur, rel_dur: (ref_hole_dur + rel_hole_dur) / (ref_dur + rel_dur) < 0.5,
    progress: tqdm.tqdm | None = None,
):
    if progress is None:
        progress = tqdm.tqdm(desc="computing_match")
    with progress:
        _, grp_start = compute_match(ref_ar_start, rel_ar_start, tol, cost_func, progress)
        _, grp_end = compute_match(ref_ar_end, rel_ar_end, tol, cost_func, progress)

    def find_start_end_matches(grp_starts, grp_ends):
        s, e = 0, 0
        groups = []
        ref_group_id_start = np.full(ref_ar_start.size, -1)
        ref_group_id_end = np.full(ref_ar_start.size, -1)
        rel_group_id_start = np.full(rel_ar_start.size, -1)
        rel_group_id_end = np.full(rel_ar_start.size, -1)

        for s in range(len(grp_starts)):
            match = False
            if e < len(grp_ends):
                try:
                    if (s + 1 >= len(grp_starts)) or (grp_ends[e][0] < grp_starts[s + 1][0]):

                        def compute_holes(starts, ends, match_s, match_e):
                            holes_start = starts[match_s + 1 : match_e + 1]
                            holes_end = ends[match_s:match_e]
                            return match_e - match_s, np.sum(holes_start - holes_end), ends[e] - starts[s]

                        n_ref_holes, ref_hole_duration, ref_duration = compute_holes(ref_ar_start, ref_ar_end, grp_starts[s][0], grp_ends[e][0])
                        n_rel_holes, rel_hole_duration, rel_duration = compute_holes(rel_ar_start, rel_ar_end, grp_starts[s][1], grp_ends[e][1])
                        if interval_func(n_ref_holes, n_rel_holes, ref_hole_duration, rel_hole_duration, ref_duration, rel_duration):
                            match = True
                except:
                    print(grp_starts)
                    print(grp_starts[s + 1])
                    raise
            if match:
                for i in range(grp_starts[s][0], grp_ends[e][0] + 1):
                    ref_group_id_start[i] = len(groups)
                    ref_group_id_end[i] = len(groups)
                for i in range(grp_starts[s][1], grp_ends[e][1] + 1):
                    rel_group_id_start[i] = len(groups)
                    rel_group_id_end[i] = len(groups)
                groups.append(
                    (
                        grp_starts[s][0],
                        grp_ends[e][0],
                        grp_starts[s][1],
                        grp_ends[e][1],
                        n_ref_holes,
                        ref_hole_duration,
                        n_rel_holes,
                        rel_hole_duration,
                    )
                )
                e += 1
            else:
                ref_group_id_start[grp_starts[s][0]] = len(groups)
                rel_group_id_start[grp_starts[s][1]] = len(groups)
                groups.append((grp_starts[s][0], -1, grp_starts[s][1], -1, np.nan, np.nan, np.nan, np.nan))
            while e < len(grp_ends) and ((s + 1 >= len(grp_starts)) or grp_ends[e][0] < grp_starts[s + 1][0]):
                ref_group_id_end[grp_ends[e][0]] = len(groups)
                rel_group_id_end[grp_ends[e][1]] = len(groups)
                groups.append((-1, grp_ends[e][0], -1, grp_ends[e][1], np.nan, np.nan, np.nan, np.nan))
                e += 1
        while e < len(grp_ends):
            ref_group_id_end[grp_ends[e][0]] = len(groups)
            rel_group_id_end[grp_ends[e][1]] = len(groups)
            groups.append((-1, grp_ends[e][0], -1, grp_ends[e][1], np.nan, np.nan, np.nan, np.nan))
            e += 1
        return (
            groups,
            np.stack([ref_group_id_start, ref_group_id_end], axis=-1),
            np.stack([rel_group_id_start, rel_group_id_end], axis=-1),
        )

    res, ref_ev, rel_ev = find_start_end_matches(grp_start, grp_end)
    ds = xr.Dataset()
    if len(res) > 0:
        ds["ev_ind"] = xr.DataArray([[[v[0], v[1]], [v[2], v[3]]] for v in res], dims=["match_group", "which", "bound"])
        ds["n_holes"] = xr.DataArray([[v[4], v[6]] for v in res], dims=["match_group", "which"])
        ds["hole_duration"] = xr.DataArray([[v[5], v[7]] for v in res], dims=["match_group", "which"])
    else:
        ds["ev_ind"] = xr.DataArray(np.full((0, 2, 2), 0, dtype=np.int64), dims=["match_group", "which", "bound"])
        ds["n_holes"] = xr.DataArray(np.full((0, 2), 0, dtype=np.int64), dims=["match_group", "which"])
        ds["hole_duration"] = xr.DataArray(np.full((0, 2), np.nan, dtype=np.float64), dims=["match_group", "which"])
    ds["which"] = ["ref", "rel"]
    ds["bound"] = ["start", "end"]
    ds["match_group"] = np.arange(len(res))

    return (
        ds,
        xr.DataArray(ref_ev, dims=["event", "bound"]).to_dataset(name="match_group"),
        xr.DataArray(rel_ev, dims=["event", "bound"]).to_dataset(name="match_group"),
    )


def get_log_colorscale(source_colorscale="plasma", lowest=6, npoints=20):
    return pc.sample_colorscale(pc.get_colorscale(source_colorscale), 1 - np.geomspace(1, 10**-lowest, npoints))


def get_plotly_config(filename):
    return {
        "scrollZoom": True,
        "displaylogo": False,
        "toImageButtonOptions": {
            "format": "svg",  # one of png, svg, jpeg, webp,
            "filename": filename,
        },
    }


def facet_distplot(
    ds: xr.Dataset,
    x: str,
    y: str,
    mode: Literal["scatter", "heatmap"] = "scatter",
    facet_row: str | None = None,
    facet_col: str | None = None,
    color: str | None = None,
    facet_row_agg: Literal["color", "pattern_shape"] = "pattern_shape",
    facet_col_agg: Literal["color", "pattern_shape"] | None = "pattern_shape",
    nbinsx: int = None,
    nbinsy: int = None,
    hoverdata=None,
):
    if facet_col is None:
        facet_col = "_facet_col"
        ds[facet_col] = xr.DataArray([0], dims=facet_col)

    if facet_row is None:
        facet_row = "_facet_row"
        ds[facet_row] = xr.DataArray([0], dims=facet_row)
        ds[facet_row] = np.array([0])

    if set(ds[facet_row].dims).intersection(set(ds[facet_col].dims)):
        raise Exception("Row and column facetting need to be distinct dimensions")
    row_grouped = list(ds.groupby(facet_row))
    col_grouped = list(ds.groupby(facet_col))
    fig = make_subplots(
        len(row_grouped) + 1,
        len(col_grouped) + 1,
        shared_xaxes="all",
        shared_yaxes=True,
        column_titles=[v[0] for v in col_grouped] + ["counts"],
        row_titles=["counts"] + [v[0] for v in row_grouped],
        vertical_spacing=0.02,
        row_heights=[1] + [2] * len(row_grouped),
        column_widths=[2] * len(col_grouped) + [1],
    )
    rows = {}
    cols = {}
    for i, (r, g) in enumerate(row_grouped):
        agg_col = facet_col if len(col_grouped) > 1 else None
        agg_color = color
        agg_pattern_shape = None
        if agg_col and facet_row_agg == "color":
            agg_color = agg_col
        if agg_col and facet_row_agg == "pattern_shape":
            agg_pattern_shape = agg_col

        hist = px.histogram(
            g.to_dataframe().reset_index(),
            y=y,
            nbins=nbinsy,
            color=agg_color,
            pattern_shape=agg_pattern_shape,
            barmode="stack",
        )
        hist.update_traces(showlegend=i == 0 and facet_col != "_facet_col")
        # if has_both_facets:
        if facet_col != "_facet_col":
            hist.update_traces(legendgroup="columns", legendgrouptitle={"text": facet_col})
        for trace in hist.select_traces():
            fig.add_trace(trace, col=len(col_grouped) + 1, row=i + 2)
        rows[r] = i
    for i, (r, g) in enumerate(col_grouped):
        agg_col = facet_row if len(row_grouped) > 1 else None
        agg_color = color
        agg_pattern_shape = None
        if agg_col and facet_col_agg == "color":
            agg_color = agg_col
        if agg_col and facet_col_agg == "pattern_shape":
            agg_pattern_shape = agg_col

        hist = px.histogram(
            g.to_dataframe().reset_index(),
            x=x,
            nbins=nbinsx,
            color=agg_color,
            pattern_shape=agg_pattern_shape,
            barmode="stack",
        )

        hist.update_traces(showlegend=i == 0 and facet_row != "_facet_row")
        if facet_row != "_facet_row":
            hist.update_traces(legendgroup="rows", legendgrouptitle={"text": facet_row})
        for trace in hist.select_traces():
            fig.add_trace(trace, col=i + 1, row=1)
        cols[r] = i
    for i, (r, gtmp) in enumerate(col_grouped):
        for j, (row, g) in enumerate(gtmp.groupby(facet_row)):
            if mode == "scatter":
                data = px.scatter(g.to_dataframe().reset_index(), x=x, y=y, color=color, trendline="lowess", hover_data=hoverdata)
            elif mode == "heatmap":
                data = px.density_heatmap(g.to_dataframe().reset_index(), x=x, y=y, nbinsx=nbinsx, nbinsy=nbinsy)
            row_index = rows[row]
            col_index = i
            data.update_traces(showlegend=i == 0 and j == 0, legendgroup="data", legendgrouptitle={"text": "data"})
            data.update_traces(showlegend=False, selector=dict(mode="lines"))
            for trace in data.select_traces():
                fig.add_trace(trace, row=row_index + 2, col=col_index + 1)

    fig.update_layout(barmode="stack", coloraxis_colorbar=dict(orientation="h"))
    fig.update_xaxes(title=x, row=len(row_grouped) + 1)
    fig.update_xaxes(title=f"{y} count", col=len(col_grouped) + 1, row=len(row_grouped) + 1)
    tmp = {k: fig.layout[k]["title"]["text"] for k in fig.layout if "xaxis" in k}
    tmp = [k for k, v in tmp.items() if v == f"{y} count"][0].replace("xaxis", "x")
    fig.update_xaxes(matches=tmp, col=len(col_grouped) + 1)

    fig.update_yaxes(title=y, col=1)
    fig.update_yaxes(title=f"{x} count", row=1, col=1)
    tmp = {k: fig.layout[k]["title"]["text"] for k in fig.layout if "yaxis" in k}
    tmp = [k for k, v in tmp.items() if v == f"{x} count"][0].replace("yaxis", "y")
    fig.update_yaxes(matches=tmp, selector=dict(title=dict(text=f"{x} count")))

    return fig


def plot_type_info(stats):
    fig = px.bar(
        stats.to_array(dim="type", name="count").to_dataframe().reset_index(),
        x="type",
        y="count",
        color="which",
        barmode="group",
        pattern_shape="bound",
        facet_col="event_name",
        text_auto=True,
    )
    return fig


def plot_time_info(matching_info, slope, shift):
    data: xr.Dataset = matching_info.copy()
    data = data.assign(
        ref_t=matching_info["t"].sel(which="ref"),
        diff=matching_info["t"].sel(which="ref") - (matching_info["t"].sel(which="rel") * slope + shift),
    )
    fig = facet_distplot(
        data.drop_dims("which"),
        facet_col="event_name",
        x="ref_t",
        y="diff",
        color="bound",
        nbinsx=50,
        nbinsy=20,
        hoverdata=["match_group"],
    )
    fig.update_traces(marker={"size": 2}, selector=dict(mode="markers"))
    return fig


def plot_matching(slope, shift, matching_info: xr.Dataset, event_ds, event_list):
    event_ds = event_ds.copy()
    event_ds["t"] = xr.where(event_ds["which"] == "rel", event_ds["t"] * slope + shift, event_ds["t"])
    event_ds["start"] = event_ds["t"].sel(bound="start")
    event_ds["end"] = event_ds["t"].sel(bound="end")
    event_ds["match_group"] = xr.where(event_ds["match_group"] < 0, np.nan, event_ds["match_group"])

    matching_info = matching_info.copy()

    matching_info["t"] = xr.where(matching_info["which"] == "rel", matching_info["t"] * slope + shift, matching_info["t"])
    fig = make_subplots(rows=len(event_list), cols=1, shared_xaxes=True, shared_yaxes=True, row_titles=event_list, vertical_spacing=0.02)
    event_to_row = {k: i + 1 for i, k in enumerate(event_list)}
    which_to_y = dict(ref=[0.8, 1.2], rel=[-0.2, 0.2])
    which_to_color = dict(ref="blue", rel="red")

    for i in range(event_ds.sizes["event"]):
        ev = event_ds.isel(event=i)
        fig.add_trace(
            go.Scatter(
                x=[
                    ev["t"].sel(bound="start").item(),
                    ev["t"].sel(bound="start").item(),
                    ev["t"].sel(bound="end").item(),
                    ev["t"].sel(bound="end").item(),
                ],
                y=[
                    which_to_y[ev["which"].item()][0],
                    which_to_y[ev["which"].item()][1],
                    which_to_y[ev["which"].item()][1],
                    which_to_y[ev["which"].item()][0],
                ],
                # showlegend=not ev["which"].item() in has_arr,
                showlegend=False,
                name="<br>".join(
                    [k + "=" + str(ev[k].item()) for k in ["event_id"]]
                    + ["start=" + str(np.round(ev["t"].sel(bound="start").item(), 4))]
                    + ["end=" + str(np.round(ev["t"].sel(bound="end").item(), 4))]
                    + ["duration=" + str(np.round(ev["t"].sel(bound="end") - ev["t"].sel(bound="start"), 4).item())]
                ),
                # name=ev["which"].item() + "_events",
                fill="toself",
                fillcolor=which_to_color[ev["which"].item()],
                # opacity=0.5,
                mode="lines",
                line=dict(width=0),
            ),
            row=event_to_row[ev["event_name"].item()],
            col=1,
        )

    for n in "ref", "rel":
        fig.add_trace(
            go.Scatter(
                x=[0, 0, 0, 0],
                y=[0, 0, 0, 0],
                showlegend=True,
                name=n + " events",
                fill="toself",
                fillcolor=which_to_color[n],
                opacity=0.5,
                mode="lines",
                line=dict(width=0),
            ),
            row=1,
            col=1,
        )

    missing_slices = {}
    event_ds = event_ds.drop_vars("event")
    for w in ["ref", "rel"]:
        ar = matching_info.sel(which=w)
        for ev, g in ar.groupby("event_name"):
            missing_slices[(ev, w)] = []
            missing_start_index = (g["ev_index"].sel(bound="end").shift(match_group=1, fill_value=-1)).to_numpy()
            missing_end_index = g["ev_index"].sel(bound="start").to_numpy()
            for s, e in zip(missing_start_index, missing_end_index):
                if (e - s > 1) and s >= 0 and e >= 0:
                    missing_slices[(ev, w)].append((s + 1, e - 1))
    n_slices = 0
    for (ev, w), g in event_ds.groupby(["event_name", "which"]):
        for s, e in missing_slices.get((ev, w), []):
            data = dict(
                start=g["t"].sel(bound="start").where(g["ev_index"] == s, drop=True).item(),
                end=g["t"].sel(bound="end").where(g["ev_index"] == e, drop=True).item(),
                n_missed=e - s + 1,
            )
            fig.add_trace(
                go.Scatter(
                    x=[data["start"], data["end"]],
                    y=[np.mean(which_to_y[w]), np.mean(which_to_y[w])],
                    hovertext="<br>".join([f"{k}=" + str(data[k]) for k in ["n_missed", "start", "end"]]),
                    line_color="black",
                    line_width=1,
                    mode="lines",
                    showlegend=n_slices == 0,
                    name="missing_slices",
                ),
                row=event_to_row[ev],
                col=1,
            )
            n_slices += 1

    matching_info["n_match_total"] = (matching_info["n_holes"] + 1).sum("which")
    diff_t = np.abs(matching_info["t"].sel(which="ref") - matching_info["t"].sel(which="rel"))
    matching_info["diff_t"] = diff_t.mean("bound")
    diff_t_max = matching_info["diff_t"].max()
    matching_info["t"] = matching_info["t"].fillna(matching_info["t"].sel(bound="start"))
    for i in range(matching_info.sizes["match_group"]):
        g = matching_info.isel(match_group=i)
        n = "primary matches" if g["n_match_total"].item() == 2 else "other matches"
        fig.add_trace(
            go.Scatter(
                x=[
                    g["t"].sel(bound="start", which="ref").item(),
                    g["t"].sel(bound="start", which="rel").item(),
                    g["t"].sel(bound="end", which="rel").item(),
                    g["t"].sel(bound="end", which="ref").item(),
                ],
                y=[which_to_y["ref"][0], which_to_y["rel"][1], which_to_y["rel"][1], which_to_y["ref"][0]],
                name="<br>".join([f"{k}=" + str(g[k].item()) for k in ["match_group", "diff_t", "n_match_total"]]),
                fill="toself",
                fillcolor=f"rgba(0, 255, 0, {0.5 + 0.5 * float(g['diff_t'] / diff_t_max)})",
                line_color="rgba(0, 0, 0, 1)",
                mode="lines",
                showlegend=False,
                line=dict(width=0 if g["n_match_total"].item() == 2 else 1),
            ),
            row=event_to_row[g["event_name"].item()],
            col=1,
        )

    for n in "primary", "other":
        fig.add_trace(
            go.Scatter(
                x=[0, 0, 0, 0],
                y=[0, 0, 0, 0],
                fill="toself",
                fillcolor="rgba(0, 255, 0, 0.3)",
                line_color="rgba(0, 0, 0, 1)",
                mode="lines",
                showlegend=True,
                name=n + " matches",
                line=dict(width=0 if n == "primary" else 1),
            ),
            row=1,
            col=1,
        )

    fig.update_yaxes(tickmode="array", tickvals=[np.mean(v) for v in which_to_y.values()], ticktext=list(which_to_y.keys()))
    return fig


def get_initial_event_ds(ref_df: pd.DataFrame, rel_df: pd.DataFrame, event_mapping: dict[str, str]):
    rel_df["event_name"] = rel_df["event_name"].astype(str).map({v: k for k, v in event_mapping.items()})

    for df in ref_df, rel_df:
        df["event_name"] = df["event_name"].astype(str)
        df.drop(df[~df["event_name"].isin(event_mapping.keys())].index, inplace=True)
        df["end"] = df["start"] + df["duration"]
        df["ev_index"] = df.assign(_count=1).groupby("event_name")["_count"].cumsum() - 1
        if "event_id" not in df:
            df["event_id"] = df["event_name"] + "_#" + df["ev_index"].astype(str)
        if df.duplicated("event_id").any():
            raise Exception("non unique event ids")
        if not (df["start"].shift(-1, fill_value=np.inf) >= df["start"]).all():
            raise Exception("Event dataframe is not sorted...")
    all_ds = pd.concat([ref_df.assign(which="ref"), rel_df.assign(which="rel")]).to_xarray().rename(index="event")
    all_ds["t"] = all_ds[["start", "end"]].to_array(dim="bound")
    all_ds = all_ds.drop_vars(["start", "end"]).set_coords(["ev_index", "event_id", "event_name", "which"])[["t"]]
    all_ds = all_ds.sel(event=all_ds["t"].notnull().all("bound"))
    return all_ds


def compute_sync(all_ds: xr.Dataset, event_mapping: dict[str, str], tolerance: float):
    n, tol, slope, min_shift, max_shift = compute_initial_sync_values(
        [all_ds["t"].sel(bound=bound, event=(all_ds["event_name"] == ev) & (all_ds["which"] == "ref")).to_numpy() for bound in ("start", "end") for ev in event_mapping],
        [all_ds["t"].sel(bound=bound, event=(all_ds["event_name"] == ev) & (all_ds["which"] == "rel")).to_numpy() for bound in ("start", "end") for ev in event_mapping],
        tol_search=(0.001, tolerance, 10**-3, 0.98),
        slope_search=(0.9999, 1.0001, 10, 10**-6),
    )
    return tol, slope, (min_shift + max_shift) / 2


def compute_matching(all_ds: xr.Dataset, event_mapping: dict[str, str], tol: float, slope: float, shift: float):
    def get_matching_for_event(ev):
        ref_arr_start = all_ds.sel(bound="start", event=(all_ds["event_name"] == ev) & (all_ds["which"] == "ref"))
        rel_arr_start = all_ds.sel(bound="start", event=(all_ds["event_name"] == ev) & (all_ds["which"] == "rel"))
        ref_arr_end = all_ds.sel(bound="end", event=(all_ds["event_name"] == ev) & (all_ds["which"] == "ref"))
        rel_arr_end = all_ds.sel(bound="end", event=(all_ds["event_name"] == ev) & (all_ds["which"] == "rel"))
        group_ds, ref_ev, rel_ev = compute_match_intervals(
            ref_arr_start["t"].to_numpy(),
            slope * rel_arr_start["t"].to_numpy() + shift,
            ref_arr_end["t"].to_numpy(),
            slope * rel_arr_end["t"].to_numpy() + shift,
            tol,
        )
        ref_ev = xr.merge([ref_ev, all_ds.sel(event=(all_ds["event_name"] == ev) & (all_ds["which"] == "ref"))])
        rel_ev = xr.merge([rel_ev, all_ds.sel(event=(all_ds["event_name"] == ev) & (all_ds["which"] == "rel"))])
        evs = xr.concat([ref_ev, rel_ev], dim="event")
        group_ds = group_ds
        tmp_ref_ds = all_ds.sel(event=(all_ds["event_name"] == ev) & (all_ds["which"] == "ref")).isel(event=group_ds["ev_ind"].sel(which="ref")).assign(which="ref")
        tmp_rel_df = all_ds.sel(event=(all_ds["event_name"] == ev) & (all_ds["which"] == "rel")).isel(event=group_ds["ev_ind"].sel(which="rel")).assign(which="rel")
        all_tmp = xr.concat([tmp_ref_ds, tmp_rel_df], dim="which")
        all_tmp = xr.where(group_ds["ev_ind"] >= 0, all_tmp, np.nan)
        group_ds = xr.merge([group_ds, all_tmp])
        if not ((group_ds["ev_ind"] == group_ds["ev_index"]) | (group_ds["ev_ind"] == -1)).all():
            raise Exception("Problem")
        group_ds = group_ds.drop_vars("ev_index").rename(ev_ind="ev_index")
        return group_ds, evs

    group_ds = []
    event_ds = []
    match_group_inc = 0
    for ev in event_mapping:
        ds, evs = get_matching_for_event(ev)
        for d in ds, evs:
            d["match_group"] = d["match_group"] + match_group_inc
        match_group_inc += ds.sizes["match_group"]
        group_ds.append(ds.assign(event_name=ev))
        event_ds.append(evs)
    group_ds = xr.concat(group_ds, dim="match_group")
    group_ds["has_bound"] = (group_ds["ev_index"] >= 0).all("which")
    event_ds = xr.concat(event_ds, dim="event")
    return group_ds, event_ds


def sync_from_matching(matching_merged: xr.Dataset):
    data = matching_merged.stack(ev=["match_group", "bound"], create_index=False)
    data = data.sel(ev=data["t"].notnull().all("which"))
    rel_t = data["t"].sel(which="rel")
    ref_t = data["t"].sel(which="ref")
    lr, residuals, rank, sing = np.linalg.lstsq(np.stack([rel_t.to_numpy(), np.ones_like(rel_t)]).T, ref_t.to_numpy())
    slope = lr[0]
    shift = lr[1]
    tol = np.abs(ref_t - (rel_t * slope + shift)).max().item()
    return tol, slope, shift


def get_matching_stats(all_ds, group_ds: xr.Dataset):
    stats = xr.Dataset()
    if len(group_ds["event_name"].dims) == 0:
        group_ds = group_ds.assign_coords(event_name=group_ds["match_group"].astype(str).str.slice(0, 0) + group_ds["event_name"])
    stats["all_events"] = all_ds["t"].notnull().groupby(["which", "event_name"]).sum("event").astype(int)
    stats["primary_matches"] = (group_ds["has_bound"].all("bound")).fillna(0).groupby("event_name").sum("match_group")
    group_ds["n_holes"] = group_ds["n_holes"].astype(float)
    stats["events_in_holes"] = (group_ds["n_holes"]).fillna(0).groupby("event_name").sum("match_group")
    stats["no_matching_bound"] = group_ds["has_bound"].groupby("event_name").sum("match_group") - group_ds["has_bound"].all("bound").groupby("event_name").sum("match_group")
    stats["missed"] = stats["all_events"] - stats["no_matching_bound"] - stats["events_in_holes"] - stats["primary_matches"]
    try:
        stats = stats.fillna(0).astype(int)
    except:
        print("Could not convert stats to integer...")
        return stats
    return stats


def get_summary_figure_html(
    event_mapping: dict[str, str],
    matching_name,
    events_matching_metadata,
    matching_info: xr.Dataset,
    stats,
    slope,
    shift,
    tol,
):
    if len(matching_info["event_name"].dims) == 0:
        matching_info = matching_info.assign_coords(event_name=matching_info["match_group"].astype(str).str.slice(0, 0) + matching_info["event_name"])
    stats = plot_type_info(stats)
    trend = plot_time_info(matching_info, slope, shift)
    matching = plot_matching(slope, shift, matching_info, events_matching_metadata, list(event_mapping.keys()))

    html = ""
    html += '<div style="height:100vh;">'
    html += f'<h3 style="text-align:center; width:100%;">{matching_name} matching from slope={slope}, shift={shift}, tol={tol}</h3>'
    html += '<div style="display:grid; gap:0;grid-template-columns: 100%; grid-template-rows: 30% 40% 30%;height:90%;">'

    l = [("stats", stats), ("trend", trend), ("matching", matching)]
    for j, (fname, fig) in enumerate(l):
        # if j < 2:
        html += f'<div id="{fname}"></div>'
    # else:
    #     html+=f'<div id="{fname}" style="grid-column: span 2;"></div>'
    html += "</div></div>"
    for j, (fname, fig) in enumerate(l):
        fig.update_layout(margin=dict(l=0, r=0, t=50, b=50))
        html += fig.to_html(config=get_plotly_config(f"{matching_name}_{fname}_plot"), include_plotlyjs=j == 0, div_id=fname)
    return html


def sync(
    reference_event_df: pd.DataFrame,
    relative_event_df: pd.DataFrame,
    event_mapping: dict[str | int, str | int],
    tolerance: float,
    progress: bool = False,
) -> tuple[dict, xr.Dataset, str]:
    show = print if progress else (lambda *args, **kwargs: None)
    event_mapping = {str(k): str(v) for k, v in event_mapping.items()}

    show("Loading initial dataframes")
    initial_event_ds = get_initial_event_ds(reference_event_df, relative_event_df, event_mapping)
    show(initial_event_ds)
    show(initial_event_ds["t"].groupby(["event_name", "which", "bound"]).count().rename("counts").to_dataframe().reset_index())
    show("Computing first sync parameters using approximation method")
    start_tol, start_slope, start_shift = compute_sync(initial_event_ds, event_mapping, tolerance)
    show(start_tol, start_slope, start_shift)
    show("Computing matching from first sync approximation and auto determined tolerance")
    start_matching_grp, start_matching_ev = compute_matching(initial_event_ds, event_mapping, start_tol, start_slope, start_shift)
    show(start_matching_grp)
    show("Computing optimal sync from matching")
    match_tol, new_slope, new_shift = sync_from_matching(start_matching_grp)
    show(match_tol, new_slope, new_shift)
    show("Computing matching from optimal sync and user tolerance")
    new_matching_grp, new_matching_ev = compute_matching(initial_event_ds, event_mapping, tolerance, new_slope, new_shift)
    show(new_matching_grp)
    show("Computing stats from matching")
    stats = get_matching_stats(initial_event_ds, new_matching_grp)
    show(stats.to_dataframe())
    show("Creating summary figure")
    fig_html = get_summary_figure_html(event_mapping, "final", new_matching_ev, new_matching_grp, stats, new_slope, new_shift, tolerance)
    data = dict(shift=new_shift, slope=new_slope)
    return data, stats, fig_html
