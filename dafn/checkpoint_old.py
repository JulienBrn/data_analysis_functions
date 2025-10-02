from __future__ import annotations
from ast import Dict
from asyncio import Task
from math import prod
from os import read, write
from pathlib import Path
import anyio.to_thread
import xarray as xr
import dask

import anyio
import shutil

import shutil
from pathlib import Path
import dask
import xarray as xr
# from dask.distributed import Client
from typing import Any
Client = Any
import tqdm.auto as tqdm
import logging
import time
import pandas as pd
import json
from .dictlogger import DictLogger
import numpy as np
import abc
import pydantic
from typing import Literal, Any, Callable
import dataclasses

logger = logging.getLogger(__name__)

class Runnable(abc.ABC):
    id: str

    @abc.abstractmethod
    def compatible(self, other: Runnable) -> bool: ... 

    @abc.abstractmethod
    def reload_func(self, load_data: Any) -> bool: ... 

class RunnerTask(abc.ABC):
    status: Literal["awaiting", "computing", "success", "fail"]
    id: str
    obj: Runnable
    

    def __init__(self, status, id, obj):
        self.status, self.id, self.obj = status, id, obj

    @abc.abstractmethod
    async def await_compute(self): ...
    @abc.abstractmethod
    async def wait(self): ...

    @property
    @abc.abstractmethod
    def load_info(self) -> Any: pass

    @property
    @abc.abstractmethod
    def run_info(self) -> Any: pass

class Runner(abc.ABC):
    @abc.abstractmethod
    async def has(self, id: str) -> RunnerTask | None: ...
    @abc.abstractmethod
    async def submit(self, id: str, obj: Runnable) -> RunnerTask: ...

@dataclasses.dataclass
class CheckpointTask:
    id: str
    status: Literal["already_computed", "processing", "submitting", "pending", "computing", "computed", "errored", "bug"]
    obj: Runnable
    run_info: Any

class Checkpoint:
    submitted_tasks: Dict[str, CheckpointTask]

    def __init__(self, viewer):
        self.submitted_tasks: Dict[str, CheckpointTask] = {}
        self.viewer = viewer

    async def checkpoint(self, obj: Runnable, runner: Runner) -> Callable[[], Any]:
        if obj.id in self.submitted_tasks and self.submitted_tasks[obj.id].status not in ["errored", "bug"]:
            raise Exception(f"Asked to recompute {obj} when it was already submitted. Your workflow is probably bugged")
        else:
            curr_task = CheckpointTask(id=obj.id, status="processing", obj=obj, run_info=None)
            self.submitted_tasks[obj.id] = curr_task

        task = await runner.has(obj.id)
        if task is None or task.status != "success":
            curr_task.status="submitting"
            self.viewer.update_view()
            try:
                task = await runner.submit(obj.id, obj)
                curr_task.status = "pending"
                self.viewer.update_view()
                await task.await_compute()
                curr_task.status = "computing"
                self.viewer.update_view()
                await task.wait()
                curr_task.status = "computed" if task.status =="success" else "errored"
            except Exception:
                curr_task.status = "bug"
                raise
        else:
            if not obj.compatible(task.obj):
                raise Exception(f"Checkpoint object {obj} has same id as previous task {task} but are not compatible")
            curr_task.status = "already_computed"
        curr_task.run_info = task.run_info
        self.viewer.update_view()
        if curr_task.status =="errored":
            raise Exception(f"Checkpointing object {obj} errored")
        return lambda: obj.reload_func(task.load_info)
    

        if task.status == "already_computed":
            pass
        else:
            new_status = await task.
        location_id, metadata = self.get_info_from_id(id)
        self.submitted_tasks[id] = dict(metadata=metadata, location_id=location_id, status="submitted", node=obj)
        if location_id in self.locations and self.locations[location_id] != id:
            raise Exception("Several tasks with different ids going to same location")
        self.locations[location_id] = id
        ex = await self.exists(location_id)
        if ex and id in self.log and self.log[id]["result"] == "success" and not metadata.get("recompute", False):
            self.submitted_tasks[id]["status"] = "already_computed"
            all_run_info = self.log[id]
        else:
            self.submitted_tasks[id]["status"] = "awaiting_compute"
            async with self.await_compute(metadata, run_options):
                self.submitted_tasks[id]["status"] = "computing"
                try:
                    json_info, run_info = await self.write(obj, location_id, run_options) 
                except Exception:
                    logger.exception(f"Task {id} failed")
                    self.submitted_tasks[id]["status"] = "errored" 
                    all_run_info = dict(result="errored") 
                    raise
                else:
                    all_run_info = dict(json_info=json_info, run_info=run_info, result="success") 
                    self.submitted_tasks[id]["status"] = "computed"
                finally:
                    self.log[id] = all_run_info
        return lambda: self.read(location_id, all_run_info["json_info"])

    @abc.abstractmethod
    async def exists(self, location_id): ...

    @abc.abstractmethod
    def get_info_from_id(self, id): ...

    @abc.abstractmethod
    def read(self, location_id, json_info): ...

    @abc.abstractmethod
    async def write(self, obj, location_id, run_options): ...

    @abc.abstractmethod
    async def await_compute(self, metadata, run_options): ...



# if self.submitted_tasks[obj.id]["obj"] == obj and self.submitted_tasks[obj.id]["status"] != "errored":
            #     raise Exception("Asking to recompute a task that was already sucessfully computed...")
            # else:
            #     raise Exception("Several tasks with same id but different parameters")
class Checkpoint(abc.ABC):
    def __init__(self, dictlogger: DictLogger):
        self.log = dictlogger
        self.submitted_tasks = {}
        self.locations={}

    async def checkpoint(self, id: str, obj, run_options=None):
        if id in self.submitted_tasks:
            if self.submitted_tasks[id]["node"] == obj and self.submitted_tasks[id]["status"] != "errored":
                raise Exception("Asking to recompute a task that was already sucessfully computed...")
            else:
                raise Exception("Several tasks with same id but different parameters")

        location_id, metadata = self.get_info_from_id(id)
        self.submitted_tasks[id] = dict(metadata=metadata, location_id=location_id, status="submitted", node=obj)
        if location_id in self.locations and self.locations[location_id] != id:
            raise Exception("Several tasks with different ids going to same location")
        self.locations[location_id] = id
        ex = await self.exists(location_id)
        if ex and id in self.log and self.log[id]["result"] == "success" and not metadata.get("recompute", False):
            self.submitted_tasks[id]["status"] = "already_computed"
            all_run_info = self.log[id]
        else:
            self.submitted_tasks[id]["status"] = "awaiting_compute"
            async with self.await_compute(metadata, run_options):
                self.submitted_tasks[id]["status"] = "computing"
                try:
                    json_info, run_info = await self.write(obj, location_id, run_options) 
                except Exception:
                    logger.exception(f"Task {id} failed")
                    self.submitted_tasks[id]["status"] = "errored" 
                    all_run_info = dict(result="errored") 
                    raise
                else:
                    all_run_info = dict(json_info=json_info, run_info=run_info, result="success") 
                    self.submitted_tasks[id]["status"] = "computed"
                finally:
                    self.log[id] = all_run_info
        return lambda: self.read(location_id, all_run_info["json_info"])

    @abc.abstractmethod
    async def exists(self, location_id): ...

    @abc.abstractmethod
    def get_info_from_id(self, id): ...

    @abc.abstractmethod
    def read(self, location_id, json_info): ...

    @abc.abstractmethod
    async def write(self, obj, location_id, run_options): ...

    @abc.abstractmethod
    async def await_compute(self, metadata, run_options): ...



        


        # curr_task = dict(status="submitted", duration=np.nan)
        # self.log[id] = curr_task
        
        # if not p.exists():
        #     p.parent.mkdir(parents=True, exist_ok=True)
        #     tmp = p.with_name(f"{tmp_prefix}{p.name}")
        #     if tmp.exists():
        #         shutil.rmtree(tmp)

        #     saved = write_callable(tmp)
        #     def compute_task(p: Path):
        #         curr_task["status"] = "computing"
        #         self.log.log_value(id, curr_task)
        #         start = time.time()
        #         try:
        #             x.compute(scheduler="threads", num_workers=10)
        #         except Exception:
        #             curr_task["status"] = "errored"
        #             raise
        #         else:
        #             curr_task["status"] = "computed"
        #         finally:
        #             end= time.time()
        #             curr_task["duration"] = end-start
        #             self.log.log_value(id, curr_task)
        #     try:
        #         await self.runner.run(compute_task, saved)
        #         shutil.move(tmp, p)
        #     except Exception as e:
        #         logger.error(f"Exception while computing {id}")
        #         print(e)
        #         raise
        # else:
        #     curr_task["status"] = "already_computed"
        #     if id in self.prev_table:
        #         curr_task["duration"] = self.prev_table[id]["duration"]
        #     self.log.log_value(id, curr_task)
        # return await anyio.to_thread.run_sync(read_callable, p)
    
# class XarrayCheckpoint(Checkpoint):
#     async def checkpoint(self, id: str, func, args=[], kwargs={}, is_dataset: bool = False):
#         def write_func(p: Path):
#             result = func(*args, **kwargs)
#             if is_dataset:
#                 result.to_zarr(p)


# limiter = anyio.CapacityLimiter(5)
# submitted_tasks = []
# prev_table = 
# log = TaskLogger(Path("../AnalysisData/durations.txt"))




# async def checkpoint(
#     p: Path,
#     client: Client,
#     write_callable,
#     read_callable,
#     tmp_prefix: str = ".tmp",
# ):
#     if p in submitted_tasks:
#         raise Exception("Problem")
    
#     p = Path(p).resolve()
#     id = str(p.relative_to(Path("../AnalysisData").resolve()))

#     curr_task = dict(status="submitted", duration=np.nan)
#     log.log_value(id, curr_task)
#     # curr_task_index = len(submitted_tasks)
#     # submitted_tasks.append(dict(status="submited"))

#     if not p.exists():
#         p.parent.mkdir(parents=True, exist_ok=True)
#         tmp = p.with_name(f"{tmp_prefix}{p.name}")
#         if tmp.exists():
#             shutil.rmtree(tmp)

#         saved = write_callable(tmp)
#         def compute_task(x):
#             curr_task["status"] = "computing"
#             log.log_value(id, curr_task)
#             # submitted_tasks[curr_task_index]["status"] = "computing"
#             start = time.time()
#             try:
#                 x.compute(scheduler="threads", num_workers=10)
#             except Exception:
#                 curr_task["status"] = "errored"
#                 raise
#             else:
#                 curr_task["status"] = "computed"
#             finally:
#                 end= time.time()
#                 curr_task["duration"] = end-start
#                 log.log_value(id, curr_task)
#             # submitted_tasks[curr_task_index]["duration"] = end-start
#             # submitted_tasks[curr_task_index]["status"] = "computed"

#             # logger.info(f"Task {p} took {end-start:.2}s to compute")
#         # submitted_tasks[curr_task_index]["status"] = "awaiting_compute"
#         try:
#             await anyio.to_thread.run_sync(compute_task, saved, limiter=limiter)
        
#         # future = client.compute(saved, sync=True)
#             shutil.move(tmp, p)
#         except Exception as e:
#             logger.error(f"Exception while computing {id}")
#             print(e)
#             raise
#         # logger.info(f"Done Computing {p}")
#     else:
#         curr_task["status"] = "already_computed"
#         if id in prev_table:
#             curr_task["duration"] = prev_table[id]["duration"]
#         log.log_value(id, curr_task)
#         # submitted_tasks[curr_task_index]["status"] = "already_computed"
#     # print(pd.DataFrame(submitted_tasks))
#     return await anyio.to_thread.run_sync(read_callable, p)


# === Specific convenience wrappers === #

async def checkpoint_delayed(delayed, p: Path, client: Client, write_func, read_func):
    return await checkpoint(
        p=p,
        client=client,
        write_callable=lambda tmp: dask.delayed(write_func)(delayed, tmp),
        read_callable=read_func,
    )



async def checkpoint_xra_zarr(obj: xr.DataArray, p: Path, client: Client, chunks, write_args=None, read_args=None):
    write_args = write_args or {}
    read_args = read_args or {}


    def write_callable(path):
        # print("write\n", obj.to_dataset(name="__data_"))
        return obj.chunk(chunks).to_zarr(path, **write_args, compute=False)
    def read_callable(path):
        res =  xr.open_dataarray(path, engine="zarr", **read_args, chunks=chunks)
        # print("read\n", res.to_dataset(name="__data_"))
        return res
    try:
        return await checkpoint(
            p=p,
            client=client,
            write_callable=write_callable,
            read_callable=read_callable,
        )
    except Exception:
        print(f"ds in\n{obj}")
        raise


async def checkpoint_xrds_zarr(obj: xr.Dataset, p: Path, client: Client, chunks, write_args=None, read_args=None):
    write_args = write_args or {}
    read_args = read_args or {}
    # print(obj.data_vars)
    # print(obj)
    def write_callable(path):
        # print("write\n", obj.data_vars)
        return obj.chunk(chunks).to_zarr(path, **write_args, compute=False)
        
    def read_callable(path):
        res =  xr.open_dataset(path, engine="zarr", **read_args, chunks=chunks)
        # print("read\n", res.data_vars)
        return res
    try:
        return await checkpoint(
            p=p,
            client=client,
            write_callable=write_callable,
            read_callable=read_callable,
        )
    except Exception:
        print(f"ds in\n{obj}")
        raise


# async def checkpoint_delayed(delayed, p: Path, client: dask.distributed.Client, write_func, read_func):
#     p = Path(p)
#     if not p.exists():
#         p.parent.mkdir(parents=True, exist_ok=True)
#         print(f"Computing {p}")
#         saved = dask.delayed(write_func)(delayed, p.with_name(".tmp"+p.name))
#         await client.submit(saved)
#         shutil.move(p.with_name(".tmp"+p.name), p)
#     return read_func(p)

# async def checkpoint_xra_zarr(obj: xr.DataArray, p: Path, client: dask.distributed.Client, write_args = {}, read_args={}):
#     p = Path(p)
#     if not p.exists():
#         print(f"Computing {p}")
#         saved = obj.to_zarr(p.with_name(".tmp"+p.name), **write_args, compute=False)
#         await client.submit(saved)
#         shutil.move(p.with_name(".tmp"+p.name), p)
#     return xr.open_dataarray(p, engine="zarr", **read_args)

# async def checkpoint_xrds_zarr(obj: xr.Dataset, p: Path, client: dask.distributed.Client, write_args = {}, read_args={}):
#     p = Path(p)
#     if not p.exists():
#         saved = obj.to_zarr(p.with_name(".tmp"+p.name), **write_args, compute=False)
#         await client.submit(saved)
#         shutil.move(p.with_name(".tmp"+p.name), p)
#     return xr.open_dataset(p, engine="zarr", **read_args)



