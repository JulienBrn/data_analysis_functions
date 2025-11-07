import xarray as xr, pandas as pd, numpy as np
from pydantic import BaseModel
import scipy.signal, re
from abc import ABC, abstractmethod
from typing import Literal, Union, List, Callable, Any, Hashable, Optional, Sequence, Set, Tuple, TypeVar, Mapping
import dask.array as da
from typing import Protocol
import xarray as xr
import dask


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
        mask = selection(*copies).compute() #copies are always datasets as we converted a to a dataset
        if set(mask.dims) != set(new_dims):
            raise ValueError(f"The callable parameter selection should return an array whose dims are exactly all duplicated dims {mask.dims} {new_dims}")
        if not isinstance(mask, xr.DataArray):
            raise ValueError("The callable parameter selection should return a xr.DataArray")
        if not mask.dtype == bool:
            raise ValueError("The callable parameter selection should return a boolean array")
        mask = mask.reset_coords(drop=True)#Perhaps check that dimensions are still aligned as in copies...
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
    

from numba import njit, guvectorize, boolean, int64, float64

# @njit
# def _compute_nan_mask_numba(obj, overlap):
#     n = len(obj)
#     res = np.zeros(n, dtype=np.bool)
#     overlap = min(overlap, n - 1)
#     i = 0
#     count = 0
#     while i < n:
#         if np.isnan(obj[i]):
#             count+=1
#             start = max(0, i - overlap)
#             end = min(n, i + overlap + 1)

#             for j in range(start, end):
#                 res[j] = 1
#         i += 1
#     return res, count

# import numpy as np
# from numba import guvectorize, 

# The core 1D function applied along the axis

@guvectorize(
    [(float64[:], int64, boolean[:], int64[:])],  # input array, overlap scalar; outputs: mask array, count array
    '(n),()->(n),()',  # core dimension 'n'; output mask same shape, count is scalar
    nopython=True
)
def _compute_nan_mask_numba(arr, overlap_scalar, mask, count_out):
    n = arr.shape[0]
    count = 0
    for i in range(n):
        mask[i] = False

    for i in range(n):
        if np.isnan(arr[i]):
            count += 1
            start = max(0, i - overlap_scalar)
            end = min(n, i + overlap_scalar + 1)
            for j in range(start, end):
                mask[j] = True

    count_out[0] = count

def map_overlap(ar: xr.DataArray, dim: str, npy_func, overlap: int, replace_nans: bool = True):
    """
    Apply a NumPy-compatible function along one dimension of an xarray DataArray
    with chunk-wise overlap handling and optional NaN propagation.

    This function wraps `xr.apply_ufunc` and `dask.map_overlap` to apply a
    custom NumPy function (`npy_func`) to each chunk of data, while ensuring
    consistency across chunk boundaries using an overlap of `overlap` elements.

    If `replace_nans=True`, any NaN values in the input are replaced with zeros
    before applying `npy_func`, and NaNs are then re-applied (expanded by the
    overlap width) using a fast Numba kernel.

    Parameters
    ----------
    ar : xr.DataArray
        Input array (may be backed by Dask).
    dim : str
        Name of the core dimension along which to apply the function.
    npy_func : callable
        Function that operates on a NumPy 1D array and returns an array of the
        same shape. It should not depend on global array state.
    overlap : int
        Number of overlapping elements to include at chunk boundaries. This
        value should be large enough to cover the neighborhood radius of the
        applied function.
    replace_nans : bool, default True
        Whether to replace NaNs with zeros before applying `npy_func`, then
        reapply NaNs to overlapping regions afterwards.

    Returns
    -------
    xr.DataArray
        The transformed DataArray with chunk boundary overlaps trimmed.
        If the array is too small (<= 2 * overlap along `dim`), returns an
        empty selection along that dimension.

    Notes
    -----
    - Internally uses `xr.apply_ufunc` and `da.map_overlap`, so it supports both
      NumPy and Dask-backed DataArrays.
    - For best performance, `npy_func` should be implemented in NumPy or Numba.
    - Overlap trimming removes `overlap` elements from both edges of the output
      along `dim`.

    Examples
    --------
    >>> def moving_mean(a):
    ...     return np.convolve(a, np.ones(3)/3, mode="same")

    >>> map_overlap(da, dim="time", npy_func=moving_mean, overlap=1)
    <xarray.DataArray (time: ...)> ...
    """
    if ar.sizes[dim] <= 2*overlap:
        return ar.isel({dim:[]})

    if replace_nans:
        def new_npy_func(chunk):
            mask, count = _compute_nan_mask_numba(chunk, overlap)
            chunk = np.nan_to_num(chunk, nan=0)
            res = npy_func(chunk)
            if (count > 0).any():
                return np.where(mask, np.nan, res)
            else:
                return res
    else:
        new_npy_func = npy_func
        
    def dask_func(f, ar):
        if isinstance(ar, da.Array):
            #Note that because we use xr.apply_ufunc, the core dimension is always the last
            return da.map_overlap(f, ar, depth={ar.ndim-1:overlap}, boundary=np.nan, trim=True, meta=np.array((), dtype=ar.dtype))
        else:
            return f(ar)
        
    res = xr.apply_ufunc(lambda ar: dask_func(new_npy_func, ar), ar, input_core_dims=[[dim]], output_core_dims=[[dim]], dask="allowed")
    return res

def chunk(ar: Union[xr.DataArray, xr.Dataset], chunks: Optional[Mapping[str, Union[int, str]]] = None, 
          preserve_numpy_coords: bool = True, **chunks_kwargs):
    """Chunk array with option to preserve numpy coordinates."""
    if preserve_numpy_coords:
        # Track which coords are numpy arrays
        numpy_coords = [c for c in ar.coords if not isinstance(ar[c].data, da.Array)]
    
    if chunks is None:
        chunks_dict = chunks_kwargs
    else:
        chunks_dict = {**chunks, **chunks_kwargs}
    
    ar = ar.chunk(chunks_dict)
    
    if preserve_numpy_coords:
        for c in numpy_coords:
            ar[c] = ar[c].compute()
    
    return ar



def xr_merge(
    left: Union[xr.DataArray, xr.Dataset],
    right: Union[xr.DataArray, xr.Dataset],
    on: Union[str, Sequence[str]],
    how: Literal["inner", "outer", "left", "right"] = "inner",
    suffixes: Tuple[str, str] = ("_x", "_y"),
    final_dim: str = None,
):
    if isinstance(on, str):
        on = [on]

    left_coords: xr.Dataset = left[on]
    right_coords = right[on]

    if len(left_coords.dims) != 1:
        raise ValueError("Only a single dimension in which to merge is accepted for 'left'")
    if len(right_coords.dims) != 1:
        raise ValueError("Only a single dimension in which to merge is accepted for 'right'")

    left_dim = list(left_coords.sizes.keys())[0]
    right_dim = list(right_coords.sizes.keys())[0]

    if final_dim is None:
        final_dim = left_dim

    # Handle name collisions
    left_vars = {v: v + suffixes[0] for v in left.data_vars if v in right.data_vars}
    right_vars = {v: v + suffixes[1] for v in right.data_vars if v in left.data_vars}
    left = left.rename(left_vars)
    right = right.rename(right_vars)

    # Create key DataFrames
    left_df = left_coords.to_dataframe().reset_index()
    right_df = right_coords.to_dataframe().reset_index()
    left_df["__left_indices"] = np.arange(len(left_df))
    right_df["__right_indices"] = np.arange(len(right_df))

    # Perform merge
    res = pd.merge(left_df, right_df, how="inner", on=on)

    # Build final indices
    left_index = res["__left_indices"].to_numpy()
    right_index = res["__right_indices"].to_numpy()

    if how in ["left", "outer"]:
        missing = np.setdiff1d(np.arange(left.sizes[left_dim]), left_index)
        left_index = np.concatenate([left_index, missing])
    if how in ["right", "outer"]:
        raise NotImplementedError("right and outer joins not implemented")
    # left_valid = ~np.isnan(left_index)
    # right_valid = ~np.isnan(right_index)

    # left_index_filled = np.where(left_valid, left_index, 0).astype(int)
    # right_index_filled = np.where(right_valid, right_index, 0).astype(int)

    # final_index_left = xr.DataArray(left_index_filled, dims=final_dim)
    # final_index_right = xr.DataArray(right_index_filled, dims=final_dim)
    left_index = xr.DataArray(left_index, dims="__tmp")
    left_index["__tmp"] = np.arange(left_index.size)
    right_index = xr.DataArray(right_index, dims="__tmp")
    right_index["__tmp"] = np.arange(right_index.size)
    left_merged = left.isel({left_dim: left_index})
    right_merged = right.isel({right_dim: right_index})

    merged = xr.merge([left_merged, right_merged], join=how)
    merged = merged.drop_vars("__tmp").rename_dims(__tmp=final_dim)
    # merged = merged.assign_coords({k: (final_dim, res[k].to_numpy()) for k in on})


    return merged

    
