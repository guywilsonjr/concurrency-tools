import os
from concurrent.futures import ProcessPoolExecutor
from typing import Literal, Optional, Any

from pydantic import BaseModel
import psutil


logical_cores_available = len(os.sched_getaffinity(0))
physical_cores = psutil.cpu_count(logical=False)

MaxWorkers = int | Literal['logical', 'physical']


class FunctionMap(BaseModel):
    function: callable
    arg_tuples: tuple
    max_workers: Optional[MaxWorkers] = None

    def get_num_workers(self) -> int:
        if self.max_workers is None:
            return physical_cores
        if isinstance(self.max_workers, int):
            return self.max_workers
        if self.max_workers == 'logical':
            return logical_cores_available
        if self.max_workers == 'physical':
            return physical_cores
        return physical_cores

    def map_all(self) -> tuple:
        return tuple([result for result in self.generate_all()])

    def generate_all(self) -> Any:
        num_workers = self.get_num_workers()

        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            return executor.map(self.function, self.arg_tuples)



