import abc
import uuid
import time
import logging
import functools
import anyio
import anyio.to_thread
from typing import Any, List, AsyncGenerator, Protocol, ClassVar, Callable
from pathlib import Path
from pydantic import BaseModel
from .utilities import create_broadcast_log, BroadcastReceiveLog, BroadcastSendLog
from .logdb import LogDB, TrackedDict
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


_curr_process_start_time = time.time()


class Runnable(Protocol):
    runnable_type: ClassVar[str]


class PythonFctRunnable(BaseModel):
    runnable_type: ClassVar[str] = "python_func"
    func: Any
    args: List[Any] = []
    kwargs: dict[str, Any] = {}


class DaskRunnable(BaseModel):
    runnable_type: ClassVar[str] = "dask_delayed"
    nodes: List[Any]  # replace Any with dask.delayed.Delayed if you want


class Task(Protocol):
    @abc.abstractmethod
    async def wait(self) -> None: ...

    @property
    @abc.abstractmethod
    def id(self) -> uuid.UUID: ...

    @abc.abstractmethod
    async def get_run_metadata(self) -> Any: ...

    @abc.abstractmethod
    def statuses(self) -> AsyncGenerator[str, None]: ...

    @abc.abstractmethod
    async def result(self) -> Any: ...


class Scheduler(Protocol):

    @property
    @abc.abstractmethod
    def supported_runnable_types(self) -> List[str]: ...

    @abc.abstractmethod
    async def submit_runnable(self, runnable: Runnable) -> Task: ...

    @abc.abstractmethod
    async def task_from_id(self, id: uuid.UUID) -> Task: ...


class LocalThreadScheduler(Scheduler):

    class LocalTask:
        id: str
        scheduler: "LocalThreadScheduler"

        # these are injected by scheduler at creation
        _done_event: anyio.Event
        _status_recv: Callable[[], BroadcastReceiveLog]

        def __init__(self, id, scheduler, _done_event, _status_recv, _result_pointer):
            self.id, self.scheduler, self._done_event, self._status_recv, self.result = id, scheduler, _done_event, _status_recv, _result_pointer
        

        async def wait(self) -> None:
            await self._done_event.wait()

        async def get_run_metadata(self) -> Any:
            return self.scheduler.runnable_info[self.id]

        async def statuses(self) -> AsyncGenerator[str, None]:
            async for status in self._status_recv():
                yield status

        async def result(self) -> Any:
            await self.wait()
            if len(self._result) ==0:
                raise Exception("No result")
            return self._result[0]

    def __init__(self, cache_path: Path, max_runnables: int = 5):
        self.limiter = anyio.CapacityLimiter(max_runnables)
        self.runnable_info: TrackedDict = LogDB(cache_path, initial_value={}).root
        for k, v in self.runnable_info.items():
            if v.get("status", "na")._value not in ["success", "cancelled", "error", "lost"]:
                print(v["status"])
                v["status"] = "lost"

    @asynccontextmanager
    async def start(self):
        async with anyio.create_task_group() as tg:
            self.tg = tg
            yield self

    @property
    def supported_runnable_types(self) -> List[str]:
        return [PythonFctRunnable.runnable_type, DaskRunnable.runnable_type]

    async def submit_runnable(self, runnable: Runnable) -> Task:
        if runnable.runnable_type == PythonFctRunnable.runnable_type:
            func = functools.partial(runnable.func, *runnable.args, **runnable.kwargs)
        elif runnable.runnable_type == DaskRunnable.runnable_type:
            import dask
            func = functools.partial(dask.compute, *runnable.nodes)
        else:
            raise ValueError(f"Unsupported runnable type: {runnable.runnable_type}")

        runnable_id = str(uuid.uuid4())

        # lifecycle signaling
        send_stream, recv_stream_factory = create_broadcast_log()
        done_event = anyio.Event()
        self.runnable_info[runnable_id] = dict(status="submitted", process=_curr_process_start_time)
        send_stream.send_nowait("submitted")
        result = []

        async def _run_runnable():
            return await self._run_func(func, runnable_id, send_stream, done_event, result)

        self.tg.start_soon(_run_runnable)
        
        return self.LocalTask(
            id=runnable_id,
            scheduler=self,
            _done_event=done_event,
            _status_recv=recv_stream_factory,
            _result_pointer=result
        )

    async def _run_func(self, func, runnable_id, status_send: BroadcastSendLog, done_event: anyio.Event, result_dest):
        async with status_send, self.limiter:
            def update_status(new_status: str):
                self.runnable_info[runnable_id]["status"] = new_status
                status_send.send_nowait(new_status)

            update_status("computing")
            self.runnable_info[runnable_id]["start_time"] = time.time()

            try:
                res = await anyio.to_thread.run_sync(func)
                result_dest.append(res)
                self.runnable_info[runnable_id]["result"] = res
                update_status("success")
            except (anyio.get_cancelled_exc_class(), KeyboardInterrupt):
                update_status("cancelled")
            except Exception:
                logger.exception("Task failed")
                update_status("error")
            finally:
                self.runnable_info[runnable_id]["end_time"] = time.time()
                done_event.set()

    async def get_runnable_info(self, id: uuid.UUID) -> Any:
        return self.runnable_info[id]
    
    async def task_from_id(self, id: uuid.UUID) -> Task: 
        return self.LocalTask(id=id, scheduler=self, _done_event=None, _status_recv = None, _result_pointer=[]) #need to change result_pointer
