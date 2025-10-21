import re
import yaml
from pathlib import Path
from typing import List
import shutil

with Path("/home/t4user/Documents/ServerApps/task_manager/config.yaml").open("r") as f:
    config = yaml.safe_load(f)

start_path_patterns = list(config["path_mappings"].values())

def get_file_pattern_from_suffix_list(suffixes):
    def mk_or_pattern(options):
        return '(('+ ")|(".join([re.escape(opt) for opt in options])+ '))'
    return '^'+ mk_or_pattern(start_path_patterns)+r'[^\\]*'+ mk_or_pattern(suffixes) + "$"

def check_output_paths(paths: Path | List[Path], overwrite):
    if isinstance(paths, Path):
        paths = [paths]
    for path in paths:
        if path.exists():
            if overwrite =="yes":
                if path.is_dir():
                    shutil.rmtree(path)
                else:
                    path.unlink()
            else:
                raise Exception("Output path already exists")
        else:
            path.parent.mkdir(parents=True, exist_ok=True)