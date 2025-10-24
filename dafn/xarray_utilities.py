import xarray as xr, pandas as pd, numpy as np
from pydantic import BaseModel
import scipy.signal, re
from abc import ABC, abstractmethod
from typing import Literal, Union, List, Callable, Any, Hashable, Optional, Sequence, Set, Tuple, TypeVar
import dask.array as da
from typing import Protocol
import xarray as xr



T = TypeVar("T", bound=Union[xr.DataArray, xr.Dataset])

class _SelectionCallable(Protocol):
    def __call__(self, *args: xr.Dataset) -> xr.DataArray: ...

def replicate_dim(
    a: T,
    dim: str,
    n: int = 2, 
    selection: Union[Literal["all"], _SelectionCallable,] = "all",
    result_type: Literal["expanded", "stacked"] = "stacked",
    keep_coords: Optional[Set[Hashable]] = None,
    drop_coords: Optional[Set[Hashable]] = None,
    dim_rename: Union[str, Sequence[str], Callable[[int, str], str]] = "{name}_{dim_index}",
    stacked_dim: str = "{name}_pair"
) -> Tuple[T, ...]:
    """
    Replicate a specified dimension of an xarray DataArray or Dataset producing new coordinates for each dimension.
    Returns an array for each replica where the dimension name/coordinates have been replaced, they are thus prepared for automatic broadcasting,
    for example, if a1 and a2 are the results,  a1 * a2 introduces the cross products.

    Optionaly applies a selection mask on which dimension pairs to generate (out of n2).
    Optionaly filters out coordinates arrays as we multiply the number by n.

    Parameters
    ----------
    a : xr.DataArray or xr.Dataset
        Input array or dataset whose dimension will be replicated.
    dim : str
        The name of the dimension to replicate.
    n : int, default 2
        The number of times to replicate the dimension.
    selection : {'all'} or callable, default 'all'
        Determines which elements of the replicated dimensions to keep.
        - 'all': keep all elements.
        - callable: a function that takes the replicated datasets as arguments
          and returns a boolean DataArray with dimensions exactly matching the 
          duplicated dimensions.

          Examples
          --------
          >>> lambda a1, a2: a1["structure"] == a2["structure"]
          >>> lambda a1, a2: (a1["chan_x"] - a2["chan_x"])**2 + (a1["chan_y"] - a2["chan_y"])**2 > 100

    result_type : {'expanded', 'stacked'}, default 'stacked'
        How to organize the replicated dimensions in the result:
        - 'expanded': return separate arrays with the duplicated dims expanded individually.
        - 'stacked': return arrays where the new dimensions are stacked into a single dimension.
    keep_coords : set of hashable, optional
        Coordinates to keep in the duplicated arrays. All other coordinates along 
        the replicated dimension are dropped unless listed here.
    drop_coords : set of hashable, optional
        Coordinates to explicitly drop in the duplicated arrays.
    dim_rename : str, sequence of str, or callable, default "{name}_{dim_index}"
        Naming scheme for the replicated dimensions:
        - str: template using `{name}` and `{dim_index}`.
        - sequence of str: must have length `n`; each string is a template for the corresponding copy.
        - callable: function taking `(i, dim_name)` and returning the new dimension name.
    stacked_dim : str, default "{name}_pair"
        Name of the stacked dimension if `result_type='stacked'`.

    Returns
    -------
    tuple of xr.DataArray or xr.Dataset
        A tuple containing `n` replicated copies of the input. If the input was a 
        DataArray, the result will be a tuple of DataArrays; if the input was a 
        Dataset, the result will be a tuple of Datasets.

    Raises
    ------
    ValueError
        If `dim` is not in the input dimensions.
        If `selection` callable returns an array with wrong dims, wrong type, or non-boolean dtype.
        If `dim_rename` sequence length does not match `n`.

    Notes
    -----
    - Coordinates along the replicated dimension are renamed according to `dim_rename`.
    - The function converts DataArrays to temporary Datasets internally to standardize 
      operations. For DataArray inputs, the output is converted back to DataArray.

    Examples
    --------
    >>> import xarray as xr
    >>> import numpy as np
    >>> da = xr.DataArray(np.arange(6).reshape(2,3), dims=('x','y'))
    >>> a1, a2 = replicate_dim(da, dim='x', n=2)
    (<xarray.DataArray (x_1: 2, y: 3)>, <xarray.DataArray (x_2: 2, y: 3)>)

    >>> a1 * a2
    <xarray.DataArray (x_1: 2, y: 3, x_2: 2)>
    """

    
    if dim not in a.dims:
        raise ValueError(f"Dimension '{dim}' not found in a. Available dimensions: {list(a.dims)}")
    def _normalize_dim_rename(dim_rename):
        if isinstance(dim_rename, str):
            return lambda i, name: dim_rename.format_map(dict(name=name, dim_index=i+1))
        elif isinstance(dim_rename, Sequence):
            if len(dim_rename) != n:
                raise ValueError("dim_rename sequence must be of length n")
            return lambda i, name: dim_rename[i].format_map(dict(name=name))
        elif callable(dim_rename):
            return dim_rename
        else:
            raise ValueError("dim_rename must be str, Sequence[str], or callable")
    renamer = _normalize_dim_rename(dim_rename)
    stacked_dim = stacked_dim.format_map(dict(name=dim))
    prev_a = a
    if isinstance(a, xr.Dataset): pass
    else:
        if a.name is None:
            a = a.to_dataset(name="__tmp")
            a_var = "__tmp"
        else:
            a_var = a.name
            a = a.to_dataset()
    new_dims = [renamer(i, dim) for i in range(n)]
    copies = [a.rename({dim:d}) for d in new_dims]

    if selection == "all":
        shape = [a.sizes[dim]] * n
        mask = xr.DataArray(np.ones(shape, dtype=bool), dims=new_dims)
    elif callable(selection):
        mask = selection(*copies) #copies are always datasets as we converted a to a dataset
        if set(mask.dims) != set(new_dims):
            raise ValueError("The callable parameter selection should return an array whose dims are exactly all duplicated dims")
        if not isinstance(mask, xr.DataArray):
            raise ValueError("The callable parameter selection should return a xr.DataArray")
        if not mask.dtype == bool:
            raise ValueError("The callable parameter selection should return a boolean array")
    else:
        raise ValueError("Unrecognized value for parameter selection")
    if keep_coords is None:
        rm_coords = []
    else:
        rm_coords = [c for c, v in a.coords.items() if not c in keep_coords]
    if drop_coords is not None:
        rm_coords +=list(drop_coords)
    rm_coords = [c for c in rm_coords if dim in a[c].dims]
    copies = [c.drop_vars(rm_coords) for c in copies]
    rename_coords = [c for c in copies[0].coords if new_dims[0] in copies[0][c].dims and c !=new_dims[0]]
    copies = [c.rename({k: renamer(i, k) for k in rename_coords}) for i, c in enumerate(copies)]

    if result_type=="expanded":
        copies = [c.where(mask.sum(d) > 0, drop=True) for c, d in zip(copies, new_dims)]
    elif result_type =="stacked":
        vars = []
        for d in new_dims:
            mask[f"__{d}_index"] = xr.DataArray(np.arange(mask.sizes[d]), dims=d)
            vars.append(f"__{d}_index")
        mask = mask.stack({"__tmp": (new_dims)}, create_index=False)
        mask = mask.where(mask, drop=True)
        mask=mask.rename(__tmp=stacked_dim)
        copies = [c.isel({d:mask[f"__{d}_index"]}).drop_vars(vars) for c, d in zip(copies, new_dims)]
    if isinstance(prev_a, xr.DataArray):
        return tuple(c[a_var] for c in copies)
    else:
        return tuple(copies)

@xr.register_dataarray_accessor("dimops")
@xr.register_dataset_accessor("dimops")
class DimOps:
    def __init__(self, xarray_obj):
        self._obj = xarray_obj

    def replicate(self, dim: str, n: int = 2, **kwargs):
        """Replicate a dimension N times (Cartesian-style)."""
        return replicate_dim(self._obj, dim=dim, n=n, **kwargs)