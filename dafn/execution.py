from pydantic import BaseModel
from typing import Any, List, Dict
from abc import ABC, abstractmethod
from pathlib import Path
import shutil
from dask.delayed import Delayed
import dask
import dask.array as da
import xarray as xr

class Task(ABC):
    @property
    @abstractmethod
    def id(self) -> str: ...

    @abstractmethod
    def compute_result_object(self) -> Any: ...

    @abstractmethod
    def has_result(self) -> bool: ...

    @abstractmethod
    def load_result(self) -> Any: ...

    @abstractmethod
    def dump_result_object(self, result_obj) -> None: ...

    @abstractmethod
    def delete_result(self) -> None: ...

class FuncTasks(BaseModel, Task, ABC):
    func: Any
    args: List[Any]
    kwargs: Dict[str, Any]

    def compute_result_object(self) -> Any: 
        return self.func(*self.args, **self.kwargs)

class PathTask(BaseModel, ABC):
    path: Path

    @property
    def tmp_path(self):
        return self.path.with_name(".tmp"+self.path.name)
    
    def has_result(self) -> bool:
        return self.path.exists()
    
    def delete_result(self) -> bool:
        shutil.rmtree(self.path)

    def finalize(self):
        if not self.tmp_path.exists():
            raise Exception("Problem")
        if self.path.exists():
            raise Exception("Problem")
        shutil.rename(self.tmp_path, self.path)

class DelayedTask(BaseModel, Task, ABC):
    @abstractmethod
    def finalize(self): ...

    def dump_result_object(self, result_obj) -> None: 
        dask.compute(result_obj)
        self.finalize()

class XarrayFuncTask(DelayedTask, PathTask, FuncTasks):
    id: str

    @abstractmethod
    def load_result(self) -> xr.DataArray | xr.Dataset:
        import xarray as xr
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

    def compute_result_object(self) -> Any: 
        obj = super().compute_result_object()
        if isinstance(obj, xr.DataArray):
            if obj.name is None:
                obj.name = "data"
            obj = obj.to_dataset()
            obj.attrs["_is_xrdatarray_"] = True
        else:
            obj.attrs["_is_xrdatarray_"] = False
        obj.attrs["_computed_"] = all(not isinstance(v.data, da.Array) for v in obj.variables.values())
        return obj.to_zarr(self.tmp_path, compute=False)

    @abstractmethod
    def dump_result_object(self, result_obj: Delayed) -> None: 
        dask.compute(result_obj)
        self.finalize()

def compute_tasks(tasks: Task | List[Task]):
    if isinstance(tasks, Task):
        tasks = [tasks]
        single=True
    else:
        single=False
    delayed_tasks = [t for t in tasks if not t.has_result() and isinstance(t, DelayedTask)]
    other_non_computed_tasks = [t for t in tasks if not t.has_result() and not isinstance(t, DelayedTask)]
    delayed_result_objects = [t.compute_result_object() for t in delayed_tasks]
    dask.compute(*delayed_result_objects)
    for t in delayed_tasks:
        t.finalize()
    for t in other_non_computed_tasks:
        obj = t.compute_result_object()
        t.dump_result_object(obj)
    res =  [t.load_result() for t in tasks]
    if single:
        return res[0]
    else:
        return res


# class Task(BaseModel, ABC):
#     id: str
#     func: Any
#     args: List[Any]
#     kwargs: Dict[str, Any]


#     def compute_result_object(self) -> Any:
#         return self.func(*self.args, **self.kwargs)
    



#     @abstractmethod
#     def has_result(self) -> bool: ...

#     def load_result(self) -> Any: 
#         if not self.has_result():
#             raise Exception("Result does not exists")
#         return self._load_result()

#     def dump_result(self, obj: Any) -> None: 
#         if self.has_result():
#             raise Exception("Result already exists")
#         self._dump_result(obj)

#     def prepare_result

#     @abstractmethod
#     def _load_result(self) -> Any: ...

#     @abstractmethod
#     def _dump_result(self, obj) -> Any: ...

#     @abstractmethod
#     def delayed_dump(self, obj) -> Delayed:
#         return dask.delayed(lambda : self._dump_result)

    


# class PathTask(Task):
#     path: Path

#     @property
#     def tmp_path(self):
#         return self.path.with_name(".tmp"+self.path.name)
    
#     def has_result(self) -> bool:
#         return self.path.exists()
    
#     def _load_result(self) -> Any: 
#         self._load_from_file(self, self.path)
        
#     def _dump_result(self, obj) -> None: 
#         self._dump_to_file(self, obj, self.tmp_path)
#         if not self.tmp_path.exists():
#             raise Exception("Problem")
#         shutil.rename(self.path, self.tmp_path)
    
#     @abstractmethod
#     def _dump_to_file(self, obj, path: Path) -> None: ...

#     @abstractmethod
#     def _load_from_file(self, path: Path) -> Any: ...


# class ZarrXarrayTask(PathTask):
#     import xarray as xr

    
#     def dump_result(self, obj: xr.DataArray | xr.Dataset) -> None:
#         import xarray as xr
#         super().dump_result()
#         if isinstance(obj, xr.DataArray):
#             if obj.name is None:
#                 obj.name = "data"
#             obj = obj.to_dataset()
#             obj.attrs["_is_xrdatarray_"] = True
#         else:
#             obj.attrs["_is_xrdatarray_"] = False
#         return obj.to_zarr(self.tmp_path, compute=False)

#     def load_result(self) -> xr.DataArray | xr.Dataset:
#         super().load_result()
#         import xarray as xr
#         ret = xr.open_zarr(self.path, compute = False)
#         if ret.attrs["_is_xrdatarray_"]:
#             vars = list(ret.data_vars)
#             if len(vars) != 1:
#                 raise Exception("Problem")
#             ret = ret[vars[0]]
#         return ret
    
# def executes_tasks