from .runnable import Scheduler, Runnable, PythonFctRunnable
import xarray as xr
from pathlib import Path
from typing import Callable, Any
import abc
from abc import abstractmethod
from pydantic import BaseModel
import dask.array as da


class Checkpoint(abc.ABC):

    @abstractmethod
    def load(self) -> Any: ...

    @abstractmethod
    async def await_computed(self) -> None: ...


class BasicCheckpoint(Checkpoint):
    path: Path
    scheduler: Scheduler
    f: Callable[[], Any]

    @abstractmethod
    def load(self) -> Any:...

    @abstractmethod
    async def await_computed(self) -> None: ...

class XarrayCheckpoint(BasicCheckpoint, BaseModel):
    f: Callable[[], xr.DataArray | xr.Dataset]

    def load(self) -> Any: 
        obj = xr.open_zarr(self.path, compute = False)
        computed, is_array = obj.attrs.pop("_computed_"), obj.attrs.pop("_is_xrdatarray_")
        if computed:
            obj = obj.compute()
        if is_array:
            vars = list(obj.data_vars)
            if len(vars) != 1:
                raise Exception("Problem")
            obj = obj[vars[0]]
        return obj

    async def await_computed(self) -> None:
        if self.path.exists():
            return 
        tmp_path = self.path.with_name(".tmp"+self.path.name)
        def task_func():
            obj = self.f()
            if isinstance(obj, xr.DataArray):
                if obj.name is None:
                    obj.name = "data"
                obj = obj.to_dataset()
                obj.attrs["_is_xrdatarray_"] = True
            else:
                obj.attrs["_is_xrdatarray_"] = False
            obj.attrs["_computed_"] = all(not isinstance(v.data, da.Array) for v in obj.variables.values())
            obj.to_zarr(tmp_path, compute=True)
        self.task = await self.scheduler.submit_runnable(PythonFctRunnable(task_func, [], {}))
        tmp_path.rename(self.path)
        async for t in self.task.statuses():
            
        await self.task.wait()

