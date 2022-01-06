from json import dumps
import os
import dataclasses
from typing import Any, Callable, Optional


class Tracer:
    def __init__(
        self,
        name: Optional[str] = None,
        obj: Optional[object] = None,
        file_prefix: str = "",
        dir_path: str = "",
    ):
        self.name: Optional[str] = name
        self.obj: Optional[object] = obj
        self._file_prefix = file_prefix
        self._dir_path = dir_path
        self._obj_name = obj.name if hasattr(obj, "name") else ""
        if name and obj:
            filename = f"{name}_{obj.__class__.__name__}_{self._obj_name}_trace.jsonl"
        elif name:
            filename = f"{name}_trace.jsonl"
        elif obj:
            filename = f"{self._file_prefix}_{obj.__class__.__name__}_{self._obj_name}_trace.jsonl"
        else:
            filename = "trace.jsonl"
        path = os.path.join(self._dir_path, filename)
        self.fd = open(path, "w", encoding="utf8")

    def dump_data(self, data: Any) -> None:
        if dataclasses.is_dataclass(data):
            if hasattr(data, "todict"):
                data = dumps(data.todict())
            else:
                data = dumps(dataclasses.asdict(data))
        elif isinstance(data, dict):
            data = dumps(data)
        else:
            data = str(data)

        self.fd.write(data + "\n")
        self.fd.flush()

    def get_trace_dumper(self, data: Any) -> Callable:
        def trace_dumper() -> None:
            return self.dump_data(data)

        return trace_dumper
