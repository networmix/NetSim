import dataclasses
import os
from json import dumps
from typing import Any, Callable, Optional


class Tracer:
    """
    A simple tracing utility that serializes data to JSON lines in a file.
    """

    def __init__(
        self,
        name: Optional[str] = None,
        obj: Optional[object] = None,
        file_prefix: str = "",
        dir_path: str = "",
    ):
        """
        Initialize a new Tracer instance.

        Args:
            name: Optional name to include in the trace file name.
            obj: Optional object whose name and type may be included in the file name.
            file_prefix: An optional prefix for the file name.
            dir_path: Directory path where the trace file will be created.
        """
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
        """
        Dump data as JSON to the trace file, each entry as a separate line.

        Args:
            data: Dataclass, dict, or any serializable Python object.
        """
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

    def get_trace_dumper(self, data: Any) -> Callable[[], None]:
        """
        Return a callable that, when invoked, will dump 'data' to the trace file.

        Args:
            data: The data object to be dumped.

        Returns:
            A zero-argument function that performs the dump.
        """

        def trace_dumper() -> None:
            self.dump_data(data)

        return trace_dumper
