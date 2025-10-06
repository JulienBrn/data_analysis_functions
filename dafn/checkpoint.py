from .runnable import Scheduler, Runnable, PythonFctRunnable, Task
import xarray as xr
from pathlib import Path
from typing import Callable, Any
import abc
from abc import abstractmethod
from pydantic import BaseModel
import dask.array as da
import shutil
import time, anyio
from .logdb import LogDB, TrackedDict

class Checkpoint(abc.ABC):
    task: Task | None

    @abstractmethod
    def load(self) -> Any: ...

    @abstractmethod
    async def compute(self) -> None: ...




class BasicCheckpoint(Checkpoint):
    path: Path
    scheduler: Scheduler
    f: Callable[[Path], Any]
    id: str
    has_task: anyio.Event
    is_scheduled: bool

    def __init__(self, id, path, scheduler, f, task=None): #The task parameter allows to reattach a running task. For now, not usefull as we run in a thread.
        self.path, self.id, self.scheduler, self.f = path, id, scheduler, f
        self.has_task = anyio.Event()
        self.task = task
        if task is not None:
            self.has_task.set()
        #We should normally check that the task is not running. For now, only a comment but if we move to out of process schedulers, this needs handling.
        self.is_scheduled=False


    def load(self) -> Any:
        if not self.path.exists():
            raise Exception("Problem, trying to load something that does not exist")
        return self._load(self.path)

    async def compute(self) -> None: 
        if self.is_scheduled:
            await self.has_task.wait()
            await self.task.wait()
        if self.path.exists():
            if self.task is not None:
                return
            shutil.rmtree(self.path)
        self.is_scheduled = True
        try:
            self.path.parent.mkdir(exist_ok=True, parents=True)
            tmp_path = self.path.with_name(".tmp"+self.path.name)
            if tmp_path.exists():
                shutil.rmtree(tmp_path)
            f = self.f
            cls = type(self)
            def task_func(tmp_path):
                obj = f()
                cls._finalize(obj, tmp_path)
            self.task = await self.scheduler.submit_runnable(PythonFctRunnable(task_func, [tmp_path], {})) #PythonFctRunnable runs in a thread
            self.has_task.set()
            await self.task.wait()
            tmp_path.rename(self.path) #We keep tmp on error so that we can investigate the error
        finally:
            self.is_scheduled=False

    @classmethod
    @abstractmethod
    def _finalize(cls, obj, path): ...

    @classmethod
    @abstractmethod
    def _load(cls, path): ...


class CheckpointRegistry:
    _FACTORY_MAP = {}

    @staticmethod
    def decorate(name: str):
        def decorator(cls):
            CheckpointRegistry._FACTORY_MAP[name] = cls
            return cls
        return decorator

    def __init__(self, db_path, scheduler: Scheduler):
        self.scheduler = scheduler
        self.state = {}
        self._registry: TrackedDict = LogDB(db_path, initial_value={}).root
        self.must_redraw = anyio.Event()

    async def create_checkpoint(self, checkpoint_type: str, f, metadata: Any) -> BasicCheckpoint:
        cls = self._FACTORY_MAP[checkpoint_type]
        id, path = self._compute_idpath(metadata)
        if id in self.state:
            raise Exception("Duplicate registering of checkpoint")
        if id in self._registry:
            if not path.exists():
                del self._registry[id]
            else:
                try:
                    task = await self.scheduler.task_from_id(self._registry[id])
                except Exception:
                    task=None
        else:
            task=None
        self.state[id]=dict(status="not submitted")
        self.must_redraw.set()
        cp = cls(id, path, self.scheduler, f, task)
        self.tg.start_soon(self.handle_checkpoint, cp)
        return cp

    async def handle_checkpoint(self, cp: BasicCheckpoint):
        try:
            await cp.has_task.wait()
            self.state[cp.id] = await cp.task.get_run_metadata()
            self._registry[cp.id] = cp.task.id
            self.must_redraw.set()
            if not cp.task.is_finished:
                async for s in cp.task.statuses:
                    self.state[cp.id]["status"] = s
                    self.must_redraw.set()
                self.state[cp.id] = await cp.task.get_run_metadata()
                self.must_redraw.set()
        except Exception as e:
            self.state[cp.id]["status"] = "cperror"
            self.must_redraw.set()

    async def run(self):
        async with anyio.create_task_group() as tg:
            self.tg = tg
            while True:
                await self.must_redraw.wait()
                self.must_redraw = anyio.Event()
                await self.redraw()
                await anyio.sleep(0.5)

    async def redraw(self):
        pass #Must draw/update state dictionary to user




class XarrayCheckpoint(BasicCheckpoint):
    f: Callable[[], xr.DataArray | xr.Dataset]

    @classmethod
    def _load(cls, path) -> Any: 
        obj = xr.open_zarr(path, compute = False)
        computed, is_array = obj.attrs.pop("_computed_"), obj.attrs.pop("_is_xrdatarray_")
        if computed:
            obj = obj.compute()
        if is_array:
            vars = list(obj.data_vars)
            if len(vars) != 1:
                raise Exception("Problem")
            obj = obj[vars[0]]
        return obj

    @classmethod
    def _finalize(cls, obj, path) -> None:
        if isinstance(obj, xr.DataArray):
            if obj.name is None:
                obj.name = "data"
            obj = obj.to_dataset()
            obj.attrs["_is_xrdatarray_"] = True
        else:
            obj.attrs["_is_xrdatarray_"] = False
        obj.attrs["_computed_"] = all(not isinstance(v.data, da.Array) for v in obj.variables.values())
        obj.to_zarr(path, compute=True)






        


            

