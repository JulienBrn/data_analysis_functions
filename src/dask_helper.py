from asyncio import Task
from math import prod
from os import read
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
from .log_df import TaskLogger
import numpy as np

logger = logging.getLogger(__name__)

limiter = anyio.CapacityLimiter(5)
submitted_tasks = []
prev_table = TaskLogger.load_table_from_file("../AnalysisData/durations.txt")
log = TaskLogger(Path("../AnalysisData/durations.txt"))




async def checkpoint(
    p: Path,
    client: Client,
    write_callable,
    read_callable,
    tmp_prefix: str = ".tmp",
):
    if p in submitted_tasks:
        raise Exception("Problem")
    
    p = Path(p).resolve()
    id = str(p.relative_to(Path("../AnalysisData").resolve()))

    curr_task = dict(status="submitted", duration=np.nan)
    log.log_value(id, curr_task)
    # curr_task_index = len(submitted_tasks)
    # submitted_tasks.append(dict(status="submited"))

    if not p.exists():
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_name(f"{tmp_prefix}{p.name}")
        if tmp.exists():
            shutil.rmtree(tmp)

        saved = write_callable(tmp)
        def compute_task(x):
            curr_task["status"] = "computing"
            log.log_value(id, curr_task)
            # submitted_tasks[curr_task_index]["status"] = "computing"
            start = time.time()
            try:
                x.compute(scheduler="threads", num_workers=10)
            except Exception:
                curr_task["status"] = "errored"
                raise
            else:
                curr_task["status"] = "computed"
            finally:
                end= time.time()
                curr_task["duration"] = end-start
                log.log_value(id, curr_task)
            # submitted_tasks[curr_task_index]["duration"] = end-start
            # submitted_tasks[curr_task_index]["status"] = "computed"

            # logger.info(f"Task {p} took {end-start:.2}s to compute")
        # submitted_tasks[curr_task_index]["status"] = "awaiting_compute"
        try:
            await anyio.to_thread.run_sync(compute_task, saved, limiter=limiter)
        
        # future = client.compute(saved, sync=True)
            shutil.move(tmp, p)
        except Exception as e:
            logger.error(f"Exception while computing {id}")
            print(e)
            raise
        # logger.info(f"Done Computing {p}")
    else:
        curr_task["status"] = "already_computed"
        if id in prev_table:
            curr_task["duration"] = prev_table[id]["duration"]
        log.log_value(id, curr_task)
        # submitted_tasks[curr_task_index]["status"] = "already_computed"
    # print(pd.DataFrame(submitted_tasks))
    return await anyio.to_thread.run_sync(read_callable, p)


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



