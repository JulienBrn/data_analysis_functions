import xarray as xr, pandas as pd, numpy as np
from pydantic import BaseModel
import scipy.signal, re
from abc import ABC, abstractmethod
from typing import Literal, Union, List, Callable, Any, Hashable, Optional, Tuple
import dask.array as da

def cartesian_broadcast(
    a: xr.DataArray,
    dim: str,
    pairs: Union[
        Literal["all"],
        Callable[[xr.DataArray, xr.DataArray], xr.DataArray],
    ] = "all",
    result_type: Literal["matrix", "stacked"] = "stacked",
    keep_coords: Optional[List[Hashable]] = None,
    drop_coords: Optional[List[Hashable]] = None,
    
    dim1_rename: Union[str, Callable[[str], str]] = "_1",
    dim2_rename: Union[str, Callable[[str], str]] = "_2",
    pairdim_name: Union[str, Callable[[str], str]] = "_pair",
) -> Tuple[xr.DataArray, xr.DataArray]:
    """
    This function allows creating another instance of a dimension

    Parameters
    ----------
    a : xr.DataArray
        the xarray
    dim : the dimension to duplicate
    pairs : {"all", "diag"} or callable, default "all"
        Specifies which channel pairs to compute.
        
        - `"all"`: compute all (dim1, dim2) combinations.
        - Callable: function `(d1, d2) -> xr.DataArray[bool]` that returns
          a boolean mask of shape `(d1, d2)` indicating which
          pairs to include. Can use coordinate of the dim dimension.
          
          Examples
          --------
          >>> lambda a1, a2: a1["structure"] == a2["structure"]
          >>> lambda a1, a2: (a1["chan_x"] - a2["chan_x"])**2 + (a1["chan_y"] - a2["chan_y"])**2 > 100
    result_type : {"matrix", "stacked"}, default "stacked"
        Determines the layout of the output data.
        
        - `"matrix"`: appends a new dimension named using dim1_rename
          The resulting array may contain NaNs if not all pairs are computed.
        - `"stacked"`: replaces the channel dimension with a single
          ``pairdim_name`` dimension containing both channel coordinates.

    keep_coords : list of hashable, optional
        Names of coordinates to keep in the output. If None, all coordinates
        are kept except those explicitly dropped.
    drop_coords : list of hashable, optional
        Names of coordinates to drop in the output. Useful when large amounts of
        metadata would otherwise be propagated.
    dim1_rename, dim2_rename : str or callable, default "_1", "_2"
        Specifies how to rename the dimensions and coordinates for the two
        channel axes. If a string, it is used as a suffix.
    pairdim_name : str or callable, default "_pair"
        Name of the new dim-pair dimension when `result_type="stacked"`.
        If a string, it is used as a suffix.

    Returns
    -------
    a1 : xr.DataArray
        First array in the broadcast pair, with dimension `dim` renamed to `dim1`.
    a2 : xr.DataArray
        Second array in the broadcast pair, with dimension `dim` renamed to `dim2`.

    Notes
    -----
      Coordinate filtering (via ``keep_coords`` and ``drop_coords``) is applied
      after pair selection.
    """
    
    # Validation
    if dim not in a.dims:
        raise ValueError(f"Dimension '{dim}' not found in DataArray. Available dimensions: {list(a.dims)}")

    a = a.copy()
    # Helper function to apply rename suffix or callable
    def apply_rename(base_name: str, rename_spec: Union[str, Callable[[str], str]]) -> str:
        if callable(rename_spec):
            return rename_spec(base_name)
        else:
            return base_name + rename_spec
    
    dim1 = apply_rename(dim, dim1_rename)
    dim2 = apply_rename(dim, dim2_rename)
    pairdim = apply_rename(dim, pairdim_name)
    
    if not dim in a.coords:
        a[dim] = np.arange(a.sizes[dim])
        created_chan_coord = True
    else:
        created_chan_coord = False


    a1 = a.rename({dim: dim1})
    a2 = a.rename({dim: dim2})

    if pairs == "all":
        mask = xr.ones_like(a1[dim1], dtype=bool)*xr.ones_like(a2[dim2], dtype=bool)
    elif callable(pairs):
        mask = pairs(a1[dim1], a2[dim2])
    else:
        raise ValueError("Wrong pair argument")
    mask = mask.drop_vars(mask.coords.keys())
    if keep_coords is None:
        rm_coords = []
    else:
        rm_coords = [c for c, v in a.coords.items() if not c in keep_coords]
    if drop_coords is not None:
        rm_coords +=list(drop_coords)
    rm_coords = [c for c in rm_coords if dim in a[c].dims]
    a1 = a1.drop_vars(rm_coords)
    a2 = a2.drop_vars(rm_coords)
    rename_coords = [c for c in a1.coords if dim1 in a1[c].dims and c !=dim1]
    a1 = a1.rename({k: apply_rename(k, dim1_rename) for k in rename_coords})
    a2 = a2.rename({k: apply_rename(k, dim2_rename) for k in rename_coords})
    if result_type=="matrix":
        a1 = a1.where(mask.sum(dim2) > 0, drop=True)
        a2 = a2.where(mask.sum(dim1) > 0, drop=True)
    elif result_type =="stacked":
        mask["__dim1_index"] =xr.DataArray(np.arange(mask.sizes[dim1]), dims=dim1)
        mask["__dim2_index"] =xr.DataArray(np.arange(mask.sizes[dim2]), dims=dim2)
        mask = mask.stack({"__tmp": (dim1, dim2)}, create_index=False)
        mask = mask.where(mask, drop=True)
        mask=mask.rename(__tmp=pairdim)
        a1 = a1.isel({dim1:mask["__dim1_index"]}).drop_vars(["__dim1_index", "__dim2_index"])
        a2 = a2.isel({dim2:mask["__dim2_index"]}).drop_vars(["__dim1_index", "__dim2_index"])
    if created_chan_coord:
        a1 = a1.drop_vars(dim1)
        a2 = a2.drop_vars(dim2)
    return a1, a2