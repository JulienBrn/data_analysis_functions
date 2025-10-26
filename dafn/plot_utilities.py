import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import xarray as xr

import numpy as np
import plotly.graph_objects as go

def add_facet_labels(
    fig: go.Figure,
    facet_row_vals,
    facet_col_vals,
    facet_row_name: str,
    facet_col_name: str,
    *,
    polar: bool = False,
    font_size: int = 12,
    offset_top: float = 0.04,
    offset_right: float = 0.00,
):
    """
    Add outer row/column labels (like Plotly Express facets) to a facet grid figure.

    Works for both polar and cartesian subplot grids created via make_subplots().

    Parameters
    ----------
    fig : go.Figure
        A Plotly figure with multiple subplots.
    facet_row_vals : list-like
        Ordered unique values along the facet rows.
    facet_col_vals : list-like
        Ordered unique values along the facet columns.
    facet_row_name : str
        Name of the row facet dimension (for annotation labels).
    facet_col_name : str
        Name of the column facet dimension (for annotation labels).
    polar : bool, default=False
        Whether subplots are polar (domains named "polar", "polar2", etc.).
    font_size : int, default=12
        Font size for annotations.
    offset_top : float, default=0.05
        Distance above the top row for column labels.
    offset_right : float, default=0.04
        Distance to the right of the last column for row labels.
    """
    rows = len(facet_row_vals)
    cols = len(facet_col_vals)

    # --- Extract subplot domains ---
    if polar:
        x_domains = [fig.layout[f"polar{j+1 if j>0 else ''}"].domain["x"] for j in range(cols)]
        y_domains = [fig.layout[f"polar{(i)*cols + 1 if i>0 else ''}"].domain["y"] for i in range(rows)]
    else:
        x_domains = [fig.layout[f"xaxis{j+1 if j>0 else ''}"].domain for j in range(cols)]
        y_domains = [fig.layout[f"yaxis{(i)*cols + 1 if i>0 else ''}"].domain for i in range(rows)]

    y_domains = y_domains[::-1]  # bottom→top order
    x_centers = np.array([(x0 + x1) / 2 for (x0, x1) in x_domains])
    y_centers = np.array([(y0 + y1) / 2 for (y0, y1) in y_domains])

    # --- Slight outward compensation for edge titles ---
    if len(x_centers) > 1:
        dx = (x_centers[1] - x_centers[0]) / 8
        x_centers[0] -= dx
        x_centers[-1] += dx
    if len(y_centers) > 1:
        dy = (y_centers[0] - y_centers[1]) / 8
        y_centers[0] += dy
        y_centers[-1] -= dy

    annotations = list(fig.layout.annotations) if fig.layout.annotations else []

    # --- Column titles (top) ---
    for j, (col_val, xmid) in enumerate(zip(facet_col_vals, x_centers)):
        annotations.append(
            dict(
                text=f"<b>{facet_col_name} = {col_val}</b>",
                x=xmid,
                y=y_domains[-1][1] + offset_top,
                xref="paper",
                yref="paper",
                showarrow=False,
                font=dict(size=font_size),
                align="center",
            )
        )

    # --- Row titles (right, vertical) ---
    for i, (row_val, ymid) in enumerate(zip(facet_row_vals, y_centers)):
        annotations.append(
            dict(
                text=f"<b>{facet_row_name} = {row_val}</b>",
                x=x_domains[-1][1] + offset_right,
                y=ymid,
                xref="paper",
                yref="paper",
                showarrow=False,
                textangle=90,
                font=dict(size=font_size),
                align="center",
            )
        )

    fig.update_layout(annotations=annotations)
    return fig



def faceted_imshow_xarray(
    data: xr.DataArray,
    r_dim: str,
    theta_dim: str,
    facet_row: str,
    facet_col: str,
    colorscale: str = "Viridis",
    subplot_height: int = 400,
    subplot_width: int = 400,
):
    """
    Faceted polar 'heatmap' using Barpolar, wrapping smoothly at theta=0/360°.
    """
    rows = len(data.coords[facet_row])
    cols = len(data.coords[facet_col])

    fig = make_subplots(
        rows=rows,
        cols=cols,
        specs=[[{"type": "polar"} for _ in range(cols)] for _ in range(rows)],
        vertical_spacing=0.07,
        horizontal_spacing=0.05
    )

    zmin = float(data.min())
    zmax = float(data.max())

    theta = np.asarray(data.coords[theta_dim].values, dtype=float)
    r = np.asarray(data.coords[r_dim].values, dtype=float)
    dtheta = np.mean(np.diff(theta)) if len(theta) > 1 else 10

    for i, r_facet in enumerate(data.coords[facet_row]):
        for j, c_facet in enumerate(data.coords[facet_col]):
            z = data.sel({facet_row: r_facet, facet_col: c_facet})

            rr, tt, zz = [], [], []
            last_r = 0
            for ri, rv in enumerate(r):
                for ti, tv in enumerate(theta):
                    rr.append(rv-last_r)
                    tt.append(tv)
                    zz.append(z.isel({r_dim:ri, theta_dim: ti}).item())
                last_r = rv
            fig.add_trace(
                go.Barpolar(
                    r=rr,
                    theta=tt,
                    width=[dtheta] * len(tt),
                    marker=dict(
                        color=zz,
                        colorscale=colorscale,
                        cmin=zmin,
                        cmax=zmax,
                        line=dict(width=0),
                        colorbar=dict(title="Value"),
                    ),
                    showlegend=False,
                ),
                row=i + 1,
                col=j + 1,
            )
    fig.update_layout(
        height=subplot_height * rows,
        width=subplot_width * cols,
        margin=dict(t=100, l=100)
    )
    fig.update_layout(
        polar=dict(
            radialaxis=dict(showticklabels=False, ticks='', showgrid=False)
        ), **{f"polar{i}": dict( radialaxis=dict(showticklabels=False, ticks='', showgrid=False)) for i in range(2, rows*cols+1)}
        
    )
    add_facet_labels(fig,data[facet_row].data,  data[facet_col].data, facet_row, facet_col, polar=True)

    return fig
