from abc import abstractmethod
from contextlib import contextmanager
import json
from pathlib import Path
from typing import Any, List, Union, Iterator
from collections.abc import MutableMapping, MutableSequence


class TrackedBase:
    """Base class for tracked dict/list with optional fine-grained logging."""

    def __init__(self, file_path: Path, key: List[Union[str, int]]):
        self._file_path = file_path
        self._key = key or []

    def _log_op(self, op: str, key: Union[str, int, None] = None, value: Any = None):
        full_key = self._key + [key] if key is not None else []
        with self._file_path.open("a") as f:
            f.write(json.dumps(dict(op=op, key=full_key, data=value))) #Note: when op is set, we add the dtype (list, dict, final) information to data

    @abstractmethod
    def get_model(self): ... # returns unwrapped object by recursively calling get_model on childs

    def apply_event(self, op, key, data): pass #Calls _apply_event on the right object

    @abstractmethod
    def _apply_event(self, op, data): ... #Applies an event (op, value) pair to the current node

    @property
    def node(self): pass #Todo allow modifications of self through obj.node = new_val

class TrackedFinalValue(TrackedBase):
    def __init__(self, file_path: Path, key: List[Union[str, int]]): pass #Todo Final values are not fine grained

class TrackedDict(TrackedBase, MutableMapping): pass #Todo

class TrackedList(TrackedBase, MutableSequence): pass #Todo
    

class LogDB:
    def __init__(self, path: Path):
        self.path = path

    def load_from_log_file(self) -> TrackedDict | TrackedList | TrackedFinalValue:
        with self.path.open("r") as f:
            data = json.loads(f.readline())
            for l in f.readlines():
                event = json.loads(l)
                #apply events to data
        return data


    def initialize(self, initial_value) -> TrackedDict | TrackedList | TrackedFinalValue:
        if self.path.exists():
            raise Exception("Expecting non existant path")
        with self.path.open("w") as f:
            f.write(json.dumps(initial_value)) #More complex needs to write the type because we can have final values in initial values for non fine grained
        return self.load_from_log_file()




