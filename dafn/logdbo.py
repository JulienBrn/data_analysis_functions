""" Module to define append only logs. No attempt at thread safety, we assume you are using async.
For now, no snapshots, sessions, transactions, logging of event times, ...
These features are planned, but lets get it working without them first.
"""

from abc import abstractmethod
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Type, Union, Callable
from collections.abc import MutableMapping, MutableSequence
import abc

# --- Registry state ---
_TRACKED_TYPES: Dict[str, Type["TrackedBase"]] = {}
_WRAP_RULES: List[tuple[Callable[[Type], float], Type["TrackedBase"]]] = []
_CACHED_TYPE_MAPPING: Dict[Type, Type["TrackedBase"]] = {}
_FIRST_WRAPPING = False


# --- Registry decorator ---
def tracked_type(type_name: str, can_wrap: Callable[[Type], float]):
    """
    Class decorator for registering tracked types.

    Args:
        type_name: string identifier (e.g., "dict", "list", "final").
        can_wrap: function that takes a Python type and returns a float priority
                  (0.0 = cannot wrap, higher = better match).
    """
    def decorator(cls):
        if _FIRST_WRAPPING:
            raise Exception(
                "Some objects have already been wrapped, cannot update wrapping rules"
            )
        cls.type_str = type_name
        _TRACKED_TYPES[type_name] = cls
        _WRAP_RULES.append((can_wrap, cls))
        return cls
    return decorator


def wrap_value(value: Any, file_path: Path, key: List[Union[str, int]]) -> "TrackedBase":
    """Wrap a value into the correct Tracked subclass according to registry."""
    if isinstance(value, TrackedBase):
        return value
    t = type(value)
    if t not in _CACHED_TYPE_MAPPING:
        global _FIRST_WRAPPING
        _FIRST_WRAPPING = True
        best_priority = 0.0
        best_cls: Optional[Type[TrackedBase]] = None
        for rule, cls in _WRAP_RULES:
            score = rule(t)
            if score > best_priority:
                best_priority = score
                best_cls = cls
        if best_cls is None:
            raise ValueError(
                f"No tracked type registered for {t}. Registered: {list(_TRACKED_TYPES.keys())}"
            )
        _CACHED_TYPE_MAPPING[t] = best_cls
    return _CACHED_TYPE_MAPPING[t].wrap(value, file_path, key)

def decode_value(encoded: dict, file_path: Path, key: list[Union[str, int]]) -> "TrackedBase":
    handler = encoded["handler"]
    cls = _TRACKED_TYPES.get(handler)
    if cls is None:
        raise ValueError(f"No tracked type registered for handler={handler}")

    value = encoded["value"]
    return cls(cls.decode(value), file_path, key)

# --- Base class ---
class TrackedBase(abc.ABC):
    """Base class for tracked dict/list/final with logging support."""

    type_str: str  # each subclass sets this via decorator
    _key: List[Any]
    _db: "LogDB"

    operations: List[str] #Each Tracked class defines a list of operations

    def __init__(self, logdb, key):
        self._key = key
        self._db = logdb

    @abstractmethod
    @classmethod
    def _construct(self, value, logdb, key): ...

    @abstractmethod
    def _apply_op(self, op, op_data): ...

    @abstractmethod
    def _encode(self, child_encode_func): ...

    @abstractmethod
    def _get_model(self, child_getmodel_func): ...

    @classmethod
    def _decode(cls, encoding, child_decode_func): ...


    def _log_op(self, op: str, opdata: Any):
        self._db._log_child_op(self._key, op, opdata)
        # event = dict(op=op, key=self._key, data=data)
        # with self._file_path.open("a") as f:
        #     f.write(json.dumps(event) + "\n")

    @classmethod
    def wrap(cls, value: Any, file_path: Path, key: List[Union[str, int]]):
        return cls(value, file_path, key)
    
    @abstractmethod
    def _apply_event(self, op: str, data: Any):
        """Apply a log event to this tracked object (in-place)."""
        ...

    def apply_event(self, event):
        """Apply a log event to this tracked object (in-place)."""
        op, key, data = event["op"], event["key"], event["data"]
        if key == []:
            self._apply_event(op, data)
        else:
            self[key[0]].apply_event(dict(op=op, key=key[1:], data=data))

    @abstractmethod
    def get_model(self):
        """Return unwrapped Python object. Should know how to recursively remove wrapped values from _value"""
        ...

    @abstractmethod
    def encode(self): 
        ...

    @abstractmethod
    @classmethod
    def decode(cls, value): 
        ...



# --- Final (scalar) ---
@tracked_type("final", can_wrap=lambda t: 0.1)
class TrackedFinalValue(TrackedBase):
    def __init__(self, value: Any, file_path: Path, key: List[Union[str, int]]):
        super().__init__(value, file_path, key)

    def get_model(self):
        return self._value

    def encode(self):
        return dict(handler=self.type_str, value=self._value)
    
    def _apply_event(self, op, data):
        raise ValueError(f"Final values do not support events: {op}")
    
    @classmethod
    def decode(cls, value): 
        return value
    
# --- Final (scalar) ---
@tracked_type("root", can_wrap=lambda t: 0.0)
class TrackedRoot(TrackedBase):
    def __init__(self, value: Any, file_path: Path):
        super().__init__(wrap_value(value, file_path, [None]), file_path, [])
        self._log_op("create_root", self._value.encode())

    def get_model(self):
        return self._value

    def encode(self):
        return dict(handler=self.type_str, value=self._value)
    
    @staticmethod
    def from_initial_ev(ev, file_path):
        if ev["op"] != "create_root":
            raise Exception("Not create root event")
        return TrackedRoot(ev["data"], file_path)
    
    def _apply_event(self, op, data):
        raise ValueError(f"Root do not have events")
    
    @classmethod
    def decode(cls, value): 
        return decode_value(value)
    
    def __getitem__(self, item):
        if item is not None:
            raise Exception("Root item should be None")
        return self._value

# --- Dict ---
@tracked_type("dict", can_wrap=lambda t: 10.0 if issubclass(t, dict) else 0.0)
class TrackedDict(TrackedBase, MutableMapping):
    _value: Dict[Any, TrackedBase]

    def __init__(self, value: dict, file_path: Path, key: List[Union[str, int]]):
        value = {k: wrap_value(v, file_path, key + [k]) for k, v in value.items()}
        super().__init__(value, file_path, key)

    def __getitem__(self, k):
        return self._value[k]

    def __setitem__(self, k, v):
        wrapped = wrap_value(v, self._file_path, self._key + [k])
        if k in self._value:
            self._log_op("replace", dict(key=k, value=wrapped.encode()))
        else:
            self._log_op("insert", dict(key=k, value=wrapped.encode()))
        self._value[k] = wrapped
        

    def __delitem__(self, k):
        self._log_op("del", dict(key=k))
        del self._value[k]

    def __iter__(self):
        return iter(self._value)

    def __len__(self):
        return len(self._value)

    def get_model(self):
        return {k: v.get_model() for k, v in self._value.items()}
    
    def encode(self):
        return dict(handler=self.type_str, value={k: v.encode() for k, v in self._value.items()})
    
    def _apply_event(self, op, data): 
        if op =="replace":
            self._value[data["key"]] = decode_value(data["value"])
        if op =="insert":
            self._value[data["key"]] = decode_value(data["value"])
        if op =="del":
            del self._value[data["key"]]

    @classmethod
    def decode(cls, value): 
        return {k: decode_value(v) for k,v in value.items()}
    
# --- LogDB ---
class LogDB(TrackedBase):
    path: Path
    _TRACKED_TYPES: Dict[str, Type["TrackedBase"]]
    _WRAP_RULES: List[tuple[Callable[[Type], float], Type["TrackedBase"]]]
    _CACHED_TYPE_MAPPING: Dict[Type, Type["TrackedBase"]] = {}
    _FIRST_WRAPPING = False
    root: TrackedBase

    def __init__(self, path: Path):
        self.path = path
        self._TRACKED_TYPES = {}
        self._WRAP_RULES = []
        self._CACHED_TYPE_MAPPING = {}
        self._FIRST_WRAPPING = False

        if not self.path.exists():
            self._initialize()
        else:
            self._load_from_log_file()

    @property
    def root(self): return self.root

    @root.setter
    def root(self, value): pass
        #Do modifications to root
        
    def _initialize(self, initial_value: Any) -> TrackedBase:
        if self.path.exists():
            raise Exception("Expecting non existent path")
        self.path.touch()
        root = TrackedRoot(initial_value, self.path)
        return root._value

    def _load_from_log_file(self) -> TrackedRoot:
        if not self.path.exists():
            raise Exception("Log file missing")

        with self.path.open("r") as f:
            create_root_ev = json.loads(f.readline())
            root = TrackedRoot.from_initial_ev(create_root_ev, self.path)
            for line in f.readlines():
                event = json.loads(line)
                root.apply_event(event)

        return root._value