# defining useful type aliases
from typing import Tuple, Union


# defining useful type aliases
SimTime = Union[int, float]
EventPriority = int
EventID = int
ProcessID = int
ProcessName = str
ResourceID = int
ResourceName = str
ResourceCapacity = Union[int, float]
TimeInterval = Tuple[SimTime]
