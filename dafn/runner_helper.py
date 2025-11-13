import re
import yaml
from pathlib import Path
from typing import List
import shutil
import contextlib
import pandas as pd

with Path("/home/t4user/Documents/ServerApps/task_manager/config.yaml").open("r") as f:
    config = yaml.safe_load(f)

start_path_patterns = list(config["path_mappings"].values())

def get_file_pattern_from_suffix_list(suffixes):
    def mk_or_pattern(options):
        return '(('+ ")|(".join([re.escape(opt) for opt in options])+ '))'
    return '^'+ mk_or_pattern(start_path_patterns)+r'[^\\]*'+ mk_or_pattern(suffixes) + "$"

# def check_output_paths(paths: Path | List[Path], overwrite):
#     if isinstance(paths, Path):
#         paths = [paths]
#     for path in paths:
#         if path.exists():
#             if overwrite =="yes":
#                 if path.is_dir():
#                     shutil.rmtree(path)
#                 else:
#                     path.unlink()
#             else:
#                 raise Exception(f"Output path {path} already exists")
#         else:
#             path.parent.mkdir(parents=True, exist_ok=True)



import shutil
import contextlib
from pathlib import Path
from typing import Union, List, Generator, TypeVar, Generic, ContextManager

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
# @contextlib.contextmanager
# def check_output_paths(paths: T, overwrite: str) -> Generator[T, None, None]:
#     is_single = isinstance(paths, Path)
#     paths = [paths] if is_single else list(paths)

#     # Handle existing final output paths
#     for path in paths:
#         if path.exists():
#             if overwrite.lower() == "yes":
#                 if path.is_dir():
#                     shutil.rmtree(path)
#                 else:
#                     path.unlink()
#             else:
#                 raise FileExistsError(f"Output path '{path}' already exists.")
#         else:
#             path.parent.mkdir(parents=True, exist_ok=True)

#     # Prepare corresponding temporary output paths
#     tmp_paths = [p.with_name(f".tmp{p.name}") for p in paths]

#     # Clean up stale tmp files if they exist
#     for tmp in tmp_paths:
#         if tmp.exists():
#             if tmp.is_dir():
#                 shutil.rmtree(tmp)
#             else:
#                 tmp.unlink()

#     try:
#         # Yield either a single tmp path or list of them
#         yield tmp_paths[0] if is_single else tmp_paths
#     except BaseException:
#         # Exception occurred: clean up temp files
#         for tmp in tmp_paths:
#             if tmp.exists():
#                 if tmp.is_dir():
#                     shutil.rmtree(tmp)
#                 else:
#                     tmp.unlink()
#         raise  # Re-raise the original exception
#     else:
#         # No exception: move tmp to final paths
#         for tmp, final in zip(tmp_paths, paths):
#             shutil.move(str(tmp), str(final))

        