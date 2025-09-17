from pathlib import Path
from typing import Callable, Dict, List, Any, Union


# --- Task object ---
class Task:
    def __init__(self, fn: Callable, name: str):
        self.fn = fn
        self.name = name
        self.inputs = None
        self.output = None

    def __call__(self, *inputs):
        self.inputs = inputs
        return self

# --- mk_task ---
def mk_task(fn: Callable, names: Union[str, List[str]]) -> Callable:
    def main_wrapper(*args, **kwargs):
        task = Task(fn, names)(*args, **kwargs)
        return task
    if isinstance(names, str):
        return main_wrapper         
    else:
        n_ret = len(names)
        def wrapper(*args, **kwargs):
            outer_task = Task(fn, "__common__")(*args, **kwargs)
            return [Task(lambda x: x[i], name=names[i])(outer_task) for i in range(n_ret)]
        return wrapper

# --- Recursive compute ---
def compute(tasks: List[Task], persist_files: Dict[str, Path], read_fn, write_fn):
    if not isinstance(tasks, list):
        tasks = [tasks]

    for task in tasks:
        # Check if outputs are already persisted
        outputs = []
        if not task.names is None:
            already_done = True
            for name in task.names:
                if name in persist_files and persist_files[name].exists():
                    outputs.append(read_fn(persist_files[name]))
                else:
                    already_done = False
            if already_done:
                task.outputs = outputs
                continue

        inputs = []
        for inp in task.inputs:
            if isinstance(inp, Task):
                compute(inp, persist_files, read_fn, write_fn)
                inputs.append(inp.outputs if len(inp.outputs) > 1 else inp.outputs[0])
            else:
                inputs.append(inp)
        if len(inputs) == 1:
            inputs = inputs[0]  # unwrap single input

        

        # Compute the task
        result = task.fn(inputs)
        if len(task.names) == 1:
            task.outputs = [result]
        else:
            task.outputs = list(result)

        # Persist outputs
        for name, output in zip(task.names, task.outputs):
            if name in persist_files:
                write_fn(persist_files[name], output)

# --- Recursive status ---
def status(tasks, persist_files: Dict[str, Path]):
    if not isinstance(tasks, list):
        tasks = [tasks]

    task_status = {}

    def _check_task(task: Task):
        # First check inputs recursively
        if task.inputs:
            for inp in task.inputs:
                if isinstance(inp, Task):
                    _check_task(inp)

        # Then check this task
        for name in task.names:
            file_path = persist_files.get(name)
            if file_path and file_path.exists():
                task_status[name] = "skipped"
            else:
                task_status[name] = "to_compute"

    for t in tasks:
        _check_task(t)

    return task_status
