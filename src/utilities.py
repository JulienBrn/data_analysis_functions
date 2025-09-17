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