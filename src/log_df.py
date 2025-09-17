import json
import time
from pathlib import Path
import pandas as pd

class TaskLogger:
    def __init__(self, logfile="task_status.jsonl"):
        self.logfile = Path(logfile)
        self.status_table = {}
        self.logfile.parent.mkdir(exist_ok=True, parents=True)
        self.logfile.touch(exist_ok=True)

    @staticmethod
    def load_table_from_file(logfile):
        logfile = Path(logfile)
        """Rebuild the in-memory status table from the JSONL file."""
        table = {}
        if logfile.exists():
            with logfile.open("r") as f:
                for line in f:
                    entry = json.loads(line)
                    task = entry["task"]
                    table[task] = entry["value"]
        return table

    def log_value(self, task, value):
        """Record a new status (in memory + append to file)."""
        entry = {
            "task": task,
            "value": value
        }
        self.status_table[task] = value
        # append to file
        with self.logfile.open("a") as f:
            f.write(json.dumps(entry) + "\n")
        print(pd.DataFrame(self.status_table).T)