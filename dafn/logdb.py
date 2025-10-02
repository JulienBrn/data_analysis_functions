""" Module to define append only logs. No attempt at thread safety. Async support will be provided later.
For now, no snapshots, sessions, transactions, logging of event times, ... 
These features are planned, but lets get it working without them first.
"""

from abc import abstractmethod
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Type, Union, Callable, Sequence, Literal
from collections.abc import MutableMapping, MutableSequence, Mapping
import abc

from attrs import has
import jsonpatch
from functools import cached_property

def _encode_tracker(tracker: "TrackedBase"):
    return (tracker.type_str, {k:_encode_tracker(c) for k,c in tracker._childs.items()}, tracker._additional_data)

def _decode_tracker(data, _tracked_types: Mapping[str, Type["TrackedBase"]]) -> "TrackedBase":
    t, c, a = data
    cls = _tracked_types[t]
    childs = {k: _decode_tracker(v, _tracked_types) for k,v in c.items()}
    r = cls._construct(childs, a)
    for k, c in r._childs.items():
        c._set_parent(r, k)
    return r


class AutoWrapper:
    _WRAP_RULES: List[tuple[Callable[[Type], float], Type["TrackedBase"]]]
    _CACHED_TYPE_MAPPING: Dict[Type, Type["TrackedBase"]]

    def __init__(self, trackers: Optional[Sequence[Type["TrackedBase"]]] = None):
        if trackers is None:
            trackers = [TrackedJsonFinalValue, TrackedDict, TrackedList]
        self._TRACKED_TYPES = {t.type_str:t for t in trackers}
        self._WRAP_RULES = [(t.wraps, t) for t in trackers]
        self._CACHED_TYPE_MAPPING = {}

    def _create(self, v) -> "TrackedBase":
        if isinstance(v, TrackedBase):
            return v
        t = type(v)
        if t not in self._CACHED_TYPE_MAPPING:
            _, cls = max(self._WRAP_RULES, key=lambda rule: rule[0](t))
            self._CACHED_TYPE_MAPPING[t] = cls
        v = self._CACHED_TYPE_MAPPING[t](v)
        return v


def mk_tracked(v, auto_wrapper: Union[AutoWrapper, Literal["default", "final"]] = "default") -> "TrackedBase":
    if auto_wrapper == "default":
        auto_wrapper = _default_auto_wrapper
    elif auto_wrapper =="final":
        return TrackedJsonFinalValue(v)
    return auto_wrapper._create(v)



# --- Base class ---
class TrackedBase(abc.ABC):
    """Base class for tracked dict/list/final with logging support."""

    # each subclass sets these values
    type_str: str  
    wraps: Callable[[Type], float]
    operations: List[str] 

    #values for each instance
    _parent: Optional["TrackedBase"]
    _parent_key: Optional[Any]
    
    @cached_property
    def _root_db(self) -> Optional["LogDB"]:
        curr = self
        while curr._parent is not None:
            curr = curr._parent
        if hasattr(curr, "_root_db_handler"):
            return curr._root_db_handler
        else:
            return None
    
    @cached_property
    def _root_path(self):
        keys = []
        curr = self
        while curr._parent is not None:
            keys.append(curr._parent_key)
            curr = curr._parent
        return keys[::-1]

    def _set_parent(self, parent: "TrackedBase", key) -> "TrackedBase":
        if self._parent is not None and self._parent != parent:
            print(self._parent)
            print(parent)
            raise Exception("Objects can not change trees for now")
        self._parent = parent
        self._parent_key = key
        self.__dict__.pop("_root_db", None)
        self.__dict__.pop("_root_path", None)
        return self
    
    def __init__(self):
        self._parent = None
        self._parent_key = None

    @property
    @abstractmethod
    def _childs(self) -> Mapping[Any, "TrackedBase"]: ...

    @property
    @abstractmethod
    def _additional_data(self) -> Any: ...
    
    @classmethod
    @abstractmethod
    def _construct(cls, childs, additional_data) -> "TrackedBase": ...

    def _logev(self, ev, data):
        if self._root_db is None:
            raise Exception("Objects need to be tracked to have events")
        self._root_db._log_op(ev, self._root_path, data)

    @abstractmethod
    def _apply_reconstruct_op(self, op, data) -> None: ...

    def __str__(self):
        if hasattr(self, "_value"):
            return "Tracked_"+self.type_str+"("+self._value.__str__()+")"
        
    def __repr__(self):
        if hasattr(self, "_value"):
            return "Tracked_"+self.type_str+"("+self._value.__repr__()+")"

class TrackedDict(TrackedBase, MutableMapping):
    type_str: str = "dict"
    operations: List[str] = ["set"]
    wraps: Callable[[Type], float] = lambda t: 1.0 if issubclass(t, dict) else 0.0

    def __init__(self, value, wrapper: AutoWrapper = None):
        if wrapper is None:
            wrapper = _default_auto_wrapper
        super().__init__()
        self._value = {k: wrapper._create(v)._set_parent(self, k) for k,v in value.items()}
        
    
    @property
    def _childs(self) -> Union[Mapping[Any, "TrackedBase"], Sequence["TrackedBase"]]: 
        return self._value
    
        
    @property
    def _additional_data(self) -> Any:
        return None
    
    @classmethod
    def _construct(cls, childs, additional_data) -> "TrackedBase": 
        return cls(childs)
    
    def __setitem__(self, k, v):
        v = mk_tracked(v)._set_parent(self, k)
        self._logev("set", (k, _encode_tracker(v)))
        self._value[k] = v

    def __getitem__(self, k): return self._value[k]
    
    def __delitem__(self, k):
        if k not in self._value:
            raise KeyError(k)
        self._logev("del", k)
        del self._value[k]

    def __iter__(self):
        return iter(self._value)

    def __len__(self):
        return len(self._value)

    # --- replay from log ---
    def _apply_reconstruct_op(self, op, data) -> None:
        if op == "set":
            (k, v) = data
            val = _decode_tracker(v, self._root_db._tracked_types)
            val._set_parent(self, k)
            self._value[k] = val
        elif op == "del":
            k = data
            if k in self._value:
                del self._value[k]
        else:
            raise Exception(f"Unknown operation {op}")

class TrackedFinalValueBase(TrackedBase):

    
    operations: List[str] = ["patch"]
    

    def __init__(self, value):
        super().__init__()
        self._value = value
    
    @property
    def value(self) -> Any:
        return self._load_value(self._dump_value())
    
    @property
    def _childs(self) -> Mapping[Any, "TrackedBase"]: 
        return {}
    
    def _apply_reconstruct_op(self, op, data):
        if op=="patch":
            self._value = self._load_value(self._apply_patch(self._dump_value(), data))
        else:
            raise Exception("Unknown operation")

    def __enter__(self):
        self.old_json = self._dump_value()
        return self._value

    def __exit__(self, exc_type, exc_value, traceback):
        self._logev("patch", self._encode_patch(self.old_json, self._dump_value()))
    
    @property
    def _additional_data(self) -> Any:
        return self._dump_value()
    
    @classmethod
    def _construct(cls, childs, additional_data) -> "TrackedBase": 
        return cls(cls._load_value(additional_data))

    @abstractmethod
    def _dump_value(self) -> str: ...
    
    @classmethod
    @abstractmethod
    def _load_value(cls, s: str) -> Any: ...
    
    @classmethod
    @abstractmethod
    def _encode_patch(cls, old_value, new_value) -> Any: ...
    
    @classmethod
    @abstractmethod
    def _apply_patch(cls, old_value, patch) -> Any: ...

class TrackedJsonFinalValue(TrackedFinalValueBase):
    type_str: str = "json_value"
    wraps: Callable[[Type], float] = lambda t: 0.1

    def __init__(self, value):
        super().__init__(value)

    def _dump_value(self) -> str: 
        return json.dumps(self._value)
    
    @classmethod
    def _load_value(cls, s: str) -> Any: 
        return json.loads(s)
    
    @classmethod
    def _encode_patch(cls, old_value, new_value) -> Any: 
        patch = jsonpatch.make_patch(old_value, new_value)
        return patch.to_string()
    
    @classmethod
    def _apply_patch(cls, old_value, patch) -> Any: 
        patch = jsonpatch.JsonPatch.from_string(patch)
        return patch.apply(old_value)

from collections.abc import Mapping

class SequenceMapping(Mapping):
    def __init__(self, sequence):
        self._sequence = sequence

    def __getitem__(self, key):
        if not isinstance(key, int):
            raise TypeError("Indices must be integers")
        try:
            return self._sequence[key]
        except IndexError:
            raise KeyError(key)

    def __iter__(self):
        return iter(range(len(self._sequence)))

    def __len__(self):
        return len(self._sequence)

def as_mapping(seq):
    return SequenceMapping(seq)

class TrackedList(TrackedBase, MutableSequence):
    type_str: str = "list"
    operations: List[str] = ["set", "insert", "del"]
    wraps: Callable[[Type], float] = lambda t: 1.0 if issubclass(t, list) else 0.0

    def __init__(self, value, wrapper: AutoWrapper = None):
        if wrapper is None:
            wrapper = _default_auto_wrapper
        super().__init__()
        self._wrapper = wrapper
        self._value: List[TrackedBase] = [
            wrapper._create(v)._set_parent(self, i) for i, v in enumerate(value)
        ]

    @property
    def _childs(self) -> Mapping[Any, "TrackedBase"]:
        return as_mapping(self._value)

    @property
    def _additional_data(self) -> Any:
        return None

    @classmethod
    def _construct(cls, childs, additional_data) -> "TrackedBase":
        return cls([childs[str(i)] for i in range(len(childs))])

    # --- list mutation operations ---
    def __setitem__(self, idx, v):
        v = mk_tracked(v)._set_parent(self, idx)
        self._logev("set", (idx, _encode_tracker(v)))
        self._value[idx] = v

    def __getitem__(self, idx):
        return self._value[idx]

    def __delitem__(self, idx):
        self._logev("del", idx)
        del self._value[idx]
        # reindex children
        for i in range(idx, len(self._value)):
            self._value[i]._parent_key = i

    def insert(self, idx, v):
        v = mk_tracked(v)._set_parent(self, idx)
        self._logev("insert", (idx, _encode_tracker(v)))
        self._value.insert(idx, v)
        # reindex children
        for i in range(idx, len(self._value)):
            self._value[i]._parent_key = i

    def __len__(self):
        return len(self._value)

    # --- replaying from log ---
    def _apply_reconstruct_op(self, op, data) -> None:
        if op == "set":
            idx, v = data
            val = _decode_tracker(v, self._root_db._tracked_types)
            val._set_parent(self, idx)
            self._value[idx] = val
        elif op == "insert":
            idx, v = data
            val = _decode_tracker(v, self._root_db._tracked_types)
            val._set_parent(self, idx)
            self._value.insert(idx, val)
            for i in range(idx, len(self._value)):
                self._value[i]._parent_key = i
        elif op == "del":
            idx = data
            del self._value[idx]
            for i in range(idx, len(self._value)):
                self._value[i]._parent_key = i
        else:
            raise Exception(f"Unknown operation {op}")

class LogDB:
    path: Path

    _TRACKED_TYPES: Dict[str, Type["TrackedBase"]]
    _current: TrackedBase


    def _log_op(self, ev, keys, data):
        with self.path.open("a") as f:
            json.dump(dict(op=ev, key=keys, data=data), f)
            f.write("\n")

    @property
    def _tracked_types(self)-> Mapping[str, Type["TrackedBase"]]:
        return self ._TRACKED_TYPES

    def __init__(self, path: Path, initial_value = None, trackers: Optional[Sequence[Type["TrackedBase"]]] = None):
        self.path = path
        if trackers is None:
            trackers = [TrackedJsonFinalValue, TrackedDict, TrackedList]
        self._TRACKED_TYPES = {t.type_str:t for t in trackers}

        self._current = TrackedJsonFinalValue(None)
        self._current._root_db_handler = self

        if self.path.exists():
            self._reconstruct_from_log()
        else:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.path.touch()
            if initial_value is not None:
                self.root = initial_value
            
            

    def _reconstruct_from_log(self):
        with self.path.open("r") as f:
            for l in f.readlines():
                print(self._current)
                load = json.loads(l)
                print(load)
                op, key, data = load["op"], load["key"], load["data"]
                if key == [] and op =="reset_root":
                    self._current = _decode_tracker(data, self._tracked_types)
                    self._current._root_db_handler = self
                else:
                    child_tracker = self._current
                    for k in key:
                        child_tracker = child_tracker._childs[k]
                    child_tracker._apply_reconstruct_op(op, data)
    
    @property 
    def root(self):
        return self._current
    
    @root.setter
    def root(self, value):
        value =  mk_tracked(value)
        value._root_db_handler = self
        value._logev("reset_root", _encode_tracker(value))
        self._current = value
        

_default_auto_wrapper = AutoWrapper()
