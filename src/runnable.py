import abc
import uuid
import time
import logging
import functools
import anyio
import anyio.to_thread
from typing import Any, List, AsyncGenerator
from pathlib import Path
from pydantic import BaseModel
from utilities import create_broadcast_log, BroadcastReceiveLog, BroadcastSendLog
from logdb import LogDB, TrackedDict

logger = logging.getLogger(__name__)


class Runnable(abc.ABC):
    runnable_type: str


class PythonFctRunnable(Runnable, BaseModel):
    runnable_type = "python_func"
    func: Any
    args: List[Any]
    kwargs: dict[str, Any]


class DaskRunnable(Runnable, BaseModel):
    runnable_type = "dask_delayed"
    nodes: List[Any]  # replace Any with dask.delayed.Delayed if you want


class Task(abc.ABC):
    @abc.abstractmethod
    async def wait(self) -> None: ...

    @property
    @abc.abstractmethod
    def id(self) -> uuid.UUID: ...

    @abc.abstractmethod
    async def get_run_metadata(self) -> Any: ...

    @abc.abstractmethod
    async def statuses(self) -> AsyncGenerator[str, None]: ...


class Scheduler(abc.ABC):

    @property
    @abc.abstractmethod
    def supported_runnable_types(self) -> List[str]: ...

    @abc.abstractmethod
    async def submit_runnable(self, runnable: Runnable) -> Task: ...

    @abc.abstractmethod
    async def task_from_id(self, id: uuid.UUID) -> Task: ...


class LocalThreadScheduler(Scheduler):

    class LocalTask(Task, BaseModel):
        id: uuid.UUID
        scheduler: "LocalThreadScheduler"

        # these are injected by scheduler at creation
        _done_event: anyio.Event
        _status_recv: BroadcastReceiveLog

        async def wait(self) -> None:
            await self._done_event.wait()

        async def get_run_metadata(self) -> Any:
            return self.scheduler.runnable_info[self.id]

        async def statuses(self) -> AsyncGenerator[str, None]:
            async for status in self._status_recv():
                yield status

    def __init__(self, cache_path: Path, max_runnables: int = 5):
        self.limiter = anyio.CapacityLimiter(max_runnables)
        self.runnable_info: TrackedDict = LogDB(cache_path, initial_value={}).root
        for k, v in self.runnable_info.items():
            if "status" in v and v["status"] not in ["success", "cancelled", "error", "lost"]:
                v["status"] = "lost"

    async def __aenter__(self):
        self._tg_cm = anyio.create_task_group()
        self.tg = await self._tg_cm.__aenter__()
        return self

    async def __aexit__(self, *exc):
        await self._tg_cm.__aexit__(*exc)

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

        runnable_id = uuid.uuid4()
        self.runnable_info[runnable_id] = dict(status="submitted")

        # lifecycle signaling
        send_stream, recv_stream_factory = create_broadcast_log()
        done_event = anyio.Event()

        await self.tg.start(self._run_func, func, runnable_id, send_stream, done_event)

        return self.LocalTask(
            id=runnable_id,
            scheduler=self,
            _done_event=done_event,
            _status_recv=recv_stream_factory,
        )

    async def _run_func(self, func, runnable_id, status_send: BroadcastSendLog, done_event: anyio.Event):
        async with status_send, self.limiter:
            def update_status(new_status: str):
                self.runnable_info[runnable_id]["status"] = new_status
                status_send.send_nowait(new_status)

            update_status("computing")
            self.runnable_info[runnable_id]["start_time"] = time.time()

            try:
                await anyio.to_thread.run_sync(func)
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
