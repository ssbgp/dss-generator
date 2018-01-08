import uuid
from typing import NamedTuple, Optional


class Simulation(NamedTuple):
    topology: str
    destination: int
    repetitions: int
    min_delay: int
    max_delay: int
    threshold: int
    stubs_file: str
    seed: Optional[int]
    reportnodes: bool = False
    id: str = str(uuid.uuid4())

    def as_dict(self) -> dict:
        return dict(self._asdict())
