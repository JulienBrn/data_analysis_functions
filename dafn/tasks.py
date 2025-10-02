from asyncio import CancelledError
import random
from pydantic import UUID4, BaseModel
from typing import Any, AsyncGenerator, Generator, List, Dict, Literal, Tuple
import abc
from pathlib import Path
import shutil
from dask.delayed import Delayed
import dask
import dask.array as da
import xarray as xr
import anyio.to_thread
import uuid
import dask.delayed
import time
import logging

logger = logging.getLogger(__name__)

class Runnable(abc.ABC):
    runnable_type: str


class PythonFctRunnable(Runnable, BaseModel):
    runnable_type  = "python_func"
    func : Any
    args: List[Any]
    kwargs : Dict[str, Any]
    


class DaskRunnable(Runnable, BaseModel):
    runnable_type = "dask_delayed"
    nodes: List[dask.delayed.Delayed]


class Task:
    @abc.abstractmethod
    async def wait(self) -> None: ...
    
    @property
    @abc.abstractmethod
    def id(self) -> uuid.UUID: ...

    @abc.abstractmethod
    async def get_run_metadata(self) -> Any: ... #Returns metadata about the run. Depends on the current status, but may contain start time, end_time, memory_usage, current_status, ...

    @abc.abstractmethod
    async def statuses(self) -> AsyncGenerator: ... #Generator os lifecycle events [submitted, computing, success | failed]


class Scheduler(abc.ABC):
    name: str

    #Responsible for task lifecycles, keeping track of task metadata, scheduling, ...
    @property
    @abc.abstractmethod
    def supported_runnable_types(self) -> List[str]: ...

    @abc.abstractmethod
    async def submit_runnable(self, runnable: Runnable) -> Task: ... #Returns a task 

    @abc.abstractmethod
    async def task_from_id(self, id: uuid.UUID) -> Task: ... #Returns a task 

    # @abc.abstractmethod
    # async def get_runnable_info(self, id: uuid.UUID) -> Any: ... #Todo, but should store runtime, logs, task_status, ...



class LocalThreadScheduler(Scheduler):

    class LocalTask(Task, BaseModel):
        id: UUID4
        async def wait(self) -> None: pass #TODO

    def __init__(self, cache_path: Path, max_runnables: int = 5):
        super().__init__()
        self.limiter = anyio.CapacityLimiter(max_runnables)
        self.runnable_info = logdb.mk_logdb(cache_path)
        self.is_running = {}
        self.tg = anyio.create_task_group()

    @property
    def supported_runnable_types(self) -> List[str]: return [PythonFctRunnable.runnable_type, DaskRunnable.runnable_type]

    async def submit_runnable(self, runnable: PythonFctRunnable) -> LocalTask:
        if runnable.runnable_type == PythonFctRunnable.runnable_type:
            func = lambda: runnable.func(*runnable.args, **runnable.kwargs)
        elif runnable.runnable_type == DaskRunnable.runnable_type:
            func = lambda: dask.compute(*runnable.nodes)
        else:
            raise Exception("Wrong runnable type")
        runnable_id = uuid.uuid4()
        self.runnable_info[runnable_id] = dict(status="submitted")
        self.tg.start_soon(self._run_func(func, runnable_id))
        return #Todo

    async def _run_func(self, func, runnable_id):
        async with self.limiter:
            self.runnable_info[runnable_id]["status"] = "computing"
            self.is_running[runnable_id] = True
            self.runnable_info[runnable_id]["start_time"] = time.time()
            try:
                await anyio.to_thread.run_sync(func)
                self.runnable_info[runnable_id]["status"] = "success"
            except CancelledError | KeyboardInterrupt:
                self.runnable_info[runnable_id]["status"]="cancelled"
            except Exception:
                logger.exception()
                self.runnable_info[runnable_id]["status"]="error"
            except:
                self.runnable_info[runnable_id]["status"]="failed"
            finally:
                self.is_running[runnable_id] = False
                self.runnable_info[runnable_id]["end_time"] = time.time()

    
    @abc.abstractmethod
    async def get_runnable_info(self, id: uuid.UUID) -> Any: 
        return self.runnable_info[id].model





class DaskThreadScheduler(Scheduler):
    @property
    def supported_runnable_types(self) -> List[str]: return [DaskRunnable.runnable_type]

    async def submit_runnable(self, runnable: DaskRunnable) -> uuid.UUID:
        task_id = uuid.uuid4()
        await anyio.to_thread.run_sync(lambda: dask.compute(*runnable.nodes))
        return task_id
        
        

class TaskGroup(abc.ABC, abc.Mapping):
    #Responsible for defining tasks. A task is computation + storage as we are in a distributed setting and objects might not fit in memory.
    @property
    @abc.abstractmethod
    def supported_runnable_types(self) -> List[str]: ...

    @abc.abstractmethod
    def build_runnable(self, graph_type: str) -> Runnable:
        """Return scheduler objects for each task.id"""

    @abc.abstractmethod
    def item_list(self) -> List[str]: ...



    def __iter__(self): return self.item_list().__iter__()

    def __len__(self): return len(self.item_list())

    @abc.abstractmethod
    def __getitem__(self, name: str) -> "Task": ...

class Task:
    #Responsible for interacting with storage (how to load, exists, delete, ...)
    tg : TaskGroup
    name: str

    @abc.abstractmethod
    def load(self) -> Any: ...

    @abc.abstractmethod
    def has_result(self) -> bool: ...

    @abc.abstractmethod
    def delete_result(self) -> None: ...

class XarrayTask(Task, BaseModel): pass #Todo

class XarrayTaskGroup(TaskGroup, BaseModel):
    func: Any
    args: List[Any]
    kwargs: Dict[str, Any]
    return_value_names: List[str]
    return_value_paths: List[Path]

    @property
    def supported_graph_types(self): return [DaskTaskGraph.graph_type]

    def build_graph(self, graph_type: str) -> DaskTaskGraph:
        nodes: List[xr.DataArray | xr.Dataset] = self.func(*self.args, **self.kwargs)
        results = [x.to_zarr(p, compute=False) for x, p in zip(nodes, self.return_value_paths)]
        return DaskTaskGraph(nodes=results, items=self.return_value_names)
        
    def item_list(self) -> List[str]: return self.return_value_names

    def __getitem__(self, name: str) -> XarrayTask: return XarrayTask(tg=self, name = name)

async def run_tasks(tasks: List[Task]) -> None: pass #Todo
async def get_tasks(tasks: List[Task]) -> List[Any]: pass #Todo gets all non computed tasks and runs them on the right scheduler. Then returns the loaded results for all of them


# class Task(ABC):
#     @property
#     @abstractmethod
#     def id(self) -> str: ...

#     @property
#     @abstractmethod
#     def scheduler(self) -> Scheduler: ...

#     @property
#     @abstractmethod
#     def build_scheduler_object(self) -> Any: ...

#     @property
#     @abstractmethod
#     def finalize(self) -> None: ...


#     @abstractmethod
#     def load(self) -> Any: ...

#     @abstractmethod
#     def has_result(self) -> bool: ...

#     @abstractmethod
#     def delete_result(self) -> None: ...

#     def run(self, overwrite: bool = False) -> None: 
#         if self.has_result():
#             if not overwrite:
#                 raise Exception("Problem, result already exists")
#             self.delete_result()
#         t = self.build_scheduler_object()
#         self.scheduler.run_tasks([t])
#         self.finalize()
    
#     def get(self):
#         if not self.has_result():
#             self.run()
#         return self.load()

        








class ComputableTaskMixin(ABC):
    @abstractmethod
    def compute_result_object(self) -> Any: ...

class FinalizableTaskMixin(ABC):
    @abstractmethod
    def dump_to_tmp(self, result_obj: Any) -> None: ...

    @abstractmethod
    def finalize(self) -> None: ...

    def dump(self) -> None:
        self.dump_to_tmp()
        result_obj = self.compute_result_object()
        self.dump_to_tmp(result_obj)
        self.finalize()