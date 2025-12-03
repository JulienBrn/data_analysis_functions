from pathlib import Path
import shutil, re, yaml
from pydantic import BaseModel, Field
from typing import List, Dict, Annotated, Tuple, Literal, Union, TypeVar, Generic, ContextManager
import xarray as xr, pandas as pd, numpy as np, dask

with Path("/mnt/data1/BirdServerStorage/Server/ServerApps/task_manager/configs/config.yaml").open("r") as f:
    config = yaml.safe_load(f)

start_path_patterns = list(config["path_mappings"].values())

def get_file_pattern_from_suffix_list(suffixes):
    def mk_or_pattern(options):
        return '(('+ ")|(".join([re.escape(opt) for opt in options])+ '))'
    return '^'+ mk_or_pattern(start_path_patterns)+r'[^\\]*'+ mk_or_pattern(suffixes) + "$"


T = TypeVar('T', Path, List[Path])

#This is a context manager
class CheckOutputPaths(Generic[T], ContextManager[T]):
    def __init__(self, paths: T, overwrite: bool):
        self.paths = paths
        self.overwrite = overwrite
        self.is_single = isinstance(paths, Path)
        self.paths_list = [paths] if self.is_single else list(paths)
        self.tmp_paths = [p.with_name(f".tmp{p.name}") for p in self.paths_list]

    def __enter__(self) -> T:
        for path in self.paths_list:
            if path.exists():
                if self.overwrite:
                    if path.is_dir():
                        shutil.rmtree(path)
                    else:
                        path.unlink()
                else:
                    raise FileExistsError(f"Output path '{path}' already exists.")
            else:
                path.parent.mkdir(parents=True, exist_ok=True)

        for tmp in self.tmp_paths:
            if tmp.exists():
                if tmp.is_dir():
                    shutil.rmtree(tmp)
                else:
                    tmp.unlink()

        return self.tmp_paths[0] if self.is_single else self.tmp_paths  # type: ignore

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            for tmp in self.tmp_paths:
                if tmp.exists():
                    if tmp.is_dir():
                        shutil.rmtree(tmp)
                    else:
                        tmp.unlink()
        else:
            for tmp, final in zip(self.tmp_paths, self.paths_list):
                shutil.move(str(tmp), str(final))

def check_output_paths(paths: T, overwrite: bool) -> CheckOutputPaths[T]:
    return CheckOutputPaths(paths, overwrite)


def finalize_events(final_df: pd.DataFrame, output_path: Path):
    print(final_df)
    print("Counts are: ")
    print(final_df.groupby("event_name").size())

    final_df.sort_values("start").to_excel(output_path, index=False)



class XarrayLoader(BaseModel):
    input_path: Annotated[Path, Field(
        description="Path to the file contining the data from which to extract events",
        default="/media/t4user/data1/Data/SpikeSorting/....xr.zarr", 
        json_schema_extra=dict(pattern=get_file_pattern_from_suffix_list([".xr.h5", ".xr.zarr"]))
    )]
    data_array_name: str | None = None
    load_method: Literal["h5", "zarr", "auto"] = "auto"
    slice_start : float | None = None
    slice_end : float | None = None

    def load(self) -> xr.DataArray:
        if self.load_method == "auto":
            if self.input_path.suffix == ".zarr":
                load_method="zarr"
            elif self.input_path.suffix == ".h5":
                load_method="h5"
            else:
                raise Exception("Unknown load method")
        else:
            load_method = self.load_method

        if load_method == "zarr":
            ds= xr.open_zarr(self.input_path)
        elif load_method == "h5":
            ds= xr.open_dataset(self.input_path)
        else:
            raise Exception("Unknown load method")
        
        with dask.diagnostics.ProgressBar():
            ds = ds.sel(t=slice(self.slice_start, self.slice_end)).compute()
        if self.data_array_name is None:
            if "__xarray_dataarray_variable__" in ds:
                return ds["__xarray_dataarray_variable__"]
            else:
                raise Exception("Array name needs to be provided")
        else:
            return ds[self.data_array_name]
