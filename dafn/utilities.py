import itertools
from dask import delayed
import dask.array as da
from dask.callbacks import Callback

from tqdm.auto import tqdm

class ProgressBar(Callback):
    def __init__(self, desc=""):
        self.desc = desc

    def _start_state(self, dsk, state):
        self._tqdm = tqdm(total=sum(len(state[k]) for k in ['ready', 'waiting', 'running', 'finished']), desc=self.desc)

    def _posttask(self, key, result, dsk, state, worker_id):
        self._tqdm.update(1)

    def _finish(self, dsk, state, errored):
        pass

def dask_array_from_chunk_function(function, shape, chunks, dtype):
    
    block_starts = [range(0, s, c) for s, c in zip(shape, chunks)]
    nchunks_per_axis = [len(b) for b in block_starts]

    delayed_blocks = []
    for start_indices in itertools.product(*block_starts):
        slices = tuple(
            slice(i, min(i + c, s))
            for i, c, s in zip(start_indices, chunks, shape)
        )
        block_shape = tuple(b.stop - b.start for b in slices)
        delayed_block = delayed(function)(slices)
        delayed_da = da.from_delayed(delayed_block, shape=block_shape, dtype=dtype)
        delayed_blocks.append(delayed_da)

    # reshape into nested list
    def reshape_nested(flat_blocks, shape):
      if len(shape) == 1:
          return [flat_blocks[i] for i in range(len(flat_blocks))]
      step = len(flat_blocks)//shape[0]
      return [
          reshape_nested(flat_blocks[i:i +step], shape[1:])
          for i in range(0, len(flat_blocks), step)
      ]
    
    nested_blocks = reshape_nested(delayed_blocks, nchunks_per_axis)
    return da.block(nested_blocks)

import anyio
from typing import Any, AsyncGenerator, List, Tuple, Callable

class BroadcastSendLog:
    def __init__(self, messages: List[Any], event: anyio.Event):
        self._messages = messages
        self._event = event
        self._finish = False

    def send_nowait(self, item: Any) -> None:
        self._messages.append(item)
        self._event.set()
        self._event = anyio.Event()

    async def send(self, item: Any) -> None:
        await anyio.sleep(0)
        self.send_nowait(item)

    async def __aenter__(self):
        pass

    async def __aexit__(self, exc_type, exc, tb):
        self._finish = True
        self._event.set()


class BroadcastReceiveLog:
    def __init__(self, ss: BroadcastSendLog, start_index: int = 0):
        self._ss = ss
        self._index = start_index

    async def __aiter__(self) -> AsyncGenerator[Any, None]:
        while True:
            # yield backlog
            while self._index < len(self._ss._messages):
                yield self._ss._messages[self._index]
                self._index += 1
            if self._ss._finish:
                break
            await self._ss._event.wait()
            
            


def create_broadcast_log() -> Tuple[BroadcastSendLog, Callable[[int], BroadcastReceiveLog]]:
    """
    Returns a send_stream and a "factory" receive_stream. all data is persisted so people can receive all information at any point.
    Each call to receive() creates an independent consumer.
    """
    messages: List[Any] = []
    event = anyio.Event()

    send = BroadcastSendLog(messages, event)

    def receive(start_index: int = 0) -> BroadcastReceiveLog:
        return BroadcastReceiveLog(send, start_index)

    return send, receive

import pandas as pd, xarray as xr, numpy as np
from typing import Union, Literal, Optional, Tuple
import collections.abc

class ValidationError(Exception): pass

Selection = Literal["all", "best:", ":best", "hungarian"]
Validation = Literal[
    "1:1", "1:m", "1:1!", "1:m!",
    "m:1", "m:m", "m:1!", "m:m!",
    "1!:1", "1!:m", "1!:1!", "1!:m!",
    "m!:1", "m!:m", "m!:1!", "m!:m!"
]

def flexible_merge(
    left: pd.DataFrame,
    right: pd.DataFrame,
    match_type: Literal["callable", "matrix", "pair_list"],
    matcher: Union[Callable[[pd.Series, pd.Series], Union[bool, float, dict]], np.ndarray],
    selection: Selection = "all",
    validation: Validation = "m:m", 
    how: Literal["inner", "outer", "left", "right"] = "inner",
    threshold: Union[float, None] = None,
    default_score: float = np.inf,
    suffixes: Tuple[str, str] = ("_x", "_y"),
) -> pd.DataFrame:
    """
    Conditionally merge two DataFrames based on a similarity or matching function.

    This function generalizes `pandas.merge` to support fuzzy, scored, or conditional matching
    between rows of two DataFrames. It separates the merging process into three stages:

    1. **Selection**: Pick candidate matches based on scores or matching logic.
    2. **Validation**: Enforce cardinality constraints (e.g., one-to-one, many-to-one).
    3. **Join**: Combine the validated matches into the final DataFrame, using standard
       join semantics (`inner`, `outer`, `left`, `right`).

    Parameters
    ----------
    left : pd.DataFrame
        Left DataFrame to merge.
    right : pd.DataFrame
        Right DataFrame to merge.
    match_type : Literal["callable", "matrix", "pair_list"]
        Type of matcher provided. Must be explicitly specified to avoid ambiguity.
    matcher : callable or array-like
        Defines how rows in `left` and `right` are considered a match:

        - **Callable**: `f(row_left, row_right) -> bool, float, or dict`
            * `bool` — True indicates a match the score used is default_score
            * `float` — numeric match score (higher = better).
            * `dict` — treated as a match; keys not starting with "_" are added as columns. 
               If a `"score"` or `"_score"` key exists, it is used as the match score otherwise the associated score is default_score.
        - **Array-like**:
            - `match_type="matrix"`: shape `(n_left, n_right)`, numeric scores.
            - `match_type="pair_list"`: shape `(m, 2)` or `(m, 3)`. First two columns
              are row indices; optional third column can contain scores or metadata.

    selection : Literal["all", "best:", ":best", "hungarian"], default "all"
        Strategy for selecting matches after thresholding:

        - `"all"` — keep all matches above `threshold`.
        - `"best:"` — select the best left match for each right row.
        - `":best"` — select the best right match for each left row.
        - `"hungarian"` — optimal one-to-one assignment maximizing total score.

    validation : Validation, default "m:m"
        Cardinality rules to enforce after selection. `!` indicates that 0 values are not allowed.
        Raises ValidationError if validation rule is not satisfied.

    how : Literal["inner", "outer", "left", "right"], default "inner"
        Join type for combining validated matches into the final DataFrame. Behaves like
        `pandas.merge`.

    threshold : float, optional
        Filters possible matchings based on score. Matchings with no score specified get the default_score value.
    default_score : float, optional
        Numeric score assigned when matcher returns a dict without a `"score"` key.
    suffixes : tuple[str, str], default ('_x', '_y')
        Suffixes applied to overlapping column names in the result.

    Returns
    -------
    pd.DataFrame
        Merged DataFrame containing rows that satisfy the matching conditions,
        augmented with any additional columns returned by the matcher.

    Notes
    -----
    - Callable matchers may perform O(n²) comparisons; for large DataFrames, use
      `matrix` or `pair_list` for efficiency.
    - Tie-breaking and selection behavior are governed by `selection`.
    - Validation ensures cardinality constraints are enforced after selection.
    - `how` determines the final join semantics (inner, outer, left, right).
    - All matcher outputs are internally converted to a canonical representation of a list of row pairs with a numeric score and metadata dictionary. The conversion rules are:
        bool — If True, an entry with score=default_score and empty metadata is created; if False, no entry is created.
        float — An entry with score=<float value> and empty metadata is created.
        dict — An entry with metadata={k: v for k, v in d.items() if not k.startswith("_")} is created, and score=d.get("score", d.get("_score", default_score)).

    Examples
    --------
    >>> def jaccard_match(a, b):
    ...     score = len(set(a.tags) & set(b.tags)) / len(set(a.tags) | set(b.tags))
    ...     return {"score": score} if score > 0 else False
    >>> conditional_merge(
    ...     df1, df2,
    ...     matcher=jaccard_match,
    ...     match_type="callable",
    ...     selection="best:",
    ...     validation="1:m",
    ...     threshold=0.5
    ... )
    """
    left_indices = np.arange(len(left.index))
    right_indices = np.arange(len(right.index))

    if match_type == "callable":
        pairs = []
        for i, (_, lrow) in enumerate(left.iterrows()):
            for j, (_, rrow) in enumerate(right.iterrows()):
                m = matcher(lrow, rrow)
                if isinstance(m, bool):
                    if m:
                        pairs.append((i, j, default_score, {}))
                elif isinstance(m, (int, float)):
                    pairs.append((i, j, m, {}))
                elif isinstance(m, collections.abc.Mapping):
                    score = m.get("score", m.get("_score", default_score))
                    pairs.append((i, j, score, {k:v for k,v in m.items() if not isinstance(k, str) or not k.startswith("_")}))
                else:
                    raise Exception("Wrong return type for matcher")
    else:
        raise NotImplementedError("Only callable match type implemented for now")
    
    pairs = pd.DataFrame(pairs, columns=["lidx", "ridx", "score", "meta"])
    if threshold is not None:
        pairs = pairs.loc[pairs["score"]>threshold]
    
    if selection=="all":
        pass
    elif selection=="best:":
        pairs = pairs.sort_values("score", ascending=False).drop_duplicates("ridx")
    elif selection==":best":
        pairs = pairs.sort_values("score", ascending=False).drop_duplicates("lidx")
    elif selection=="hungarian":
        raise NotImplementedError("Hungarian selection is not implemented for now")

    [leftv, rightv] = validation.split(":")
    
    if leftv[0] == "1":
        if pairs.duplicated("lidx").any():
            raise ValidationError("Several matches for left dataframe")
    if rightv[0] == "1":
        if pairs.duplicated("ridx").any():
            raise ValidationError("Several matches for right dataframe")
    if leftv.endswith("!"):
        if not np.isin(left_indices, pairs["lidx"]).all():
            raise ValidationError("Missing matches in left dataframe")
    if rightv.endswith("!"):
        if not np.isin(right_indices, pairs["ridx"]).all():
            raise ValidationError("Missing matches in right dataframe")
        
    common_names = set(left.columns).intersection(set(right.columns))
    left = left.rename(columns={k: k+suffixes[0] for k in common_names})
    right = right.rename(columns={k: k+suffixes[1] for k in common_names})

    metadata = pd.DataFrame(pairs["meta"].tolist())
    innerleft = left.iloc[pairs["lidx"].values].reset_index(drop=True)
    innerright = right.iloc[pairs["ridx"].values].reset_index(drop=True)

    # display(innerleft)
    # display(innerright)
    # display(metadata)

    concatenated = pd.concat([innerleft, innerright, metadata], axis=1)
    if how=="inner":
        return concatenated
    elif how=="left":
        missing = np.setdiff1d(left_indices, pairs["lidx"])
        return pd.concat([concatenated, left.iloc[missing]], axis=0)
    elif how=="right":
        missing = np.setdiff1d(right_indices, pairs["ridx"])
        return pd.concat([concatenated, right.iloc[missing]], axis=0)
    elif how=="outer":
        right_missing = np.setdiff1d(right_indices, pairs["ridx"])
        left_missing = np.setdiff1d(left_indices, pairs["lidx"])
        return pd.concat([concatenated, right.iloc[right_missing], left.iloc[left_missing]], axis=0)
        

def get_subsequence_positions(sub, a, tol=1e-6, max_candidates=50):
    """
    Find all starting indices in a where sub occurs within tolerance tol.
    Combines early pruning with a fully vectorized, memory-efficient final check.
    """
    sub = np.asarray(sub)
    a = np.asarray(a)

    if sub.size > a.size:
        return []

    n_a = a.size
    n_sub = sub.size
    candidates = np.ones(n_a, dtype=bool)
    candidates[-n_sub+1:] = False  # cannot start a full subsequence here

    # Early pruning
    for i in range(n_sub):
        candidates[:n_a - i] &= np.abs(a[i:] - sub[i]) < tol
        if candidates.sum() < max_candidates:
            break

    idxs = np.flatnonzero(candidates)
    if idxs.size == 0:
        return []

    # Vectorized precise check using direct fancy indexing
    cols = np.arange(n_sub)
    windows = a[idxs[:, None] + cols[None, :]]
    matches = np.all(np.abs(windows - sub) < tol, axis=1)

    return idxs[matches].tolist()
    

# def get_subsequence_positions(sub, a, tol=10**(-6)):
#     if sub.size > a.size:
#         return []
#     candidates = np.ones(a.size, dtype=bool)
#     i= 0
#     while i<sub.size:
#         candidates = candidates & (np.abs(np.roll(a, -i) - sub[i]) < tol)
#         sum = candidates.sum()
#         if sum < 50:
#             break
#         i+=1
            
#     candidates = np.flatnonzero(candidates)
#     res = []
#     for i in candidates:
#         if (a.size - i) < sub.size:
#             break
#         if (np.abs(a[i:i+sub.size] - sub) < tol).all():
#             res.append(i)
#     return res
    
    