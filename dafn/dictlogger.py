import json
import os
import tempfile
from pathlib import Path
import warnings
from collections.abc import MutableMapping


class DictLogger(MutableMapping):
    def __init__(self, logfile: Path, compact_log: bool = False):
        self.logfile = Path(logfile)
        self.logfile.parent.mkdir(exist_ok=True, parents=True)
        self.logfile.touch(exist_ok=True)

        self._dict = self.load_table_from_file(self.logfile)

        if compact_log:
            self.compact_log()

    @staticmethod
    def load_table_from_file(logfile, ignore_errors: bool = False):
        logfile = Path(logfile)
        _dict = {}
        if logfile.exists():
            with logfile.open("r") as f:
                for line in f:
                    try:
                        entry = json.loads(line)
                        key = entry["id"]
                        if entry.get("deleted"):
                            _dict.pop(key, None)
                        else:
                            _dict[key] = entry["value"]
                    except json.JSONDecodeError:
                        if ignore_errors:
                            warnings.warn("Errors while decoding some logs")
                        else:
                            raise
        return _dict

    def log_value(self, key, value):
        """Append a new value to the log and update memory."""
        with self.logfile.open("a") as f:
            f.write(json.dumps(dict(id=key, value=value)) + "\n")
        self._dict[key] = value

    def log_tombstone(self, key):
        """Append a tombstone to the log and remove from memory."""
        with self.logfile.open("a") as f:
            f.write(json.dumps(dict(id=key, deleted=True)) + "\n")
        self._dict.pop(key, None)

    def compact_log(self):
        """Rewrite the file with only current values (drop tombstones) atomically."""
        dest_file = self.logfile
        tmpfile = self.logfile.with_name(".tmp"+dest_file.name)
        with tmpfile.open("w") as f:
            for k, v in self._dict.items():
                f.write(json.dumps({"id": k, "value": v}) + "\n")
        os.replace(tmpfile, dest_file)  # atomic swap

    # --- required methods for MutableMapping ---
    def __getitem__(self, key):
        return self._dict[key]

    def __setitem__(self, key, value):
        self.log_value(key, value)

    def __delitem__(self, key):
        self.log_tombstone(key)

    def __iter__(self):
        return iter(self._dict)

    def __len__(self):
        return len(self._dict)

    def __contains__(self, key):
        return key in self._dict

    def clear(self):
        """Clear all keys by writing tombstones."""
        keys = list(self._dict.keys())
        for key in keys:
            self.log_tombstone(key)

    def __repr__(self):
        return f"{self.__class__.__name__}({self._dict!r}, logfile={self.logfile!s})"
