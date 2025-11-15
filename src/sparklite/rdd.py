from __future__ import annotations
import uuid
from collections.abc import Iterable
from typing import Any, Callable, Iterable, Iterator, TypeVar

T = TypeVar("T")
U = TypeVar("U")

class RDD:
    """RDD abstract base class."""
    __slots__ = ('id', 'op', 'parents', 'num_of_partitions', '_locked')
    def __init__(self, op: str, parents: tuple, num_of_partitions: int):
        if not isinstance(parents, tuple):
            raise ValueError("Parents attribute must be a tuple.")
        if not num_of_partitions >= 1:
            raise ValueError("Number of partitions attribute must be greater or equal to 1.")

        object.__setattr__(self, 'id', uuid.uuid4().hex)
        object.__setattr__(self, 'op', op)
        object.__setattr__(self, 'parents', parents)
        object.__setattr__(self, 'num_of_partitions', num_of_partitions)
        object.__setattr__(self, '_locked', True)

    def __setattr__(self, key, value):
        if getattr(self, '_locked', False):
            raise AttributeError("An RDD is an immutable object.")
        object.__setattr__(self, key, value)

    def compute(self, partition_index: int) -> Iterator[Any]:
        NotImplementedError("Method not yet implemented.")

    def map(self, function: Callable[[T], U]) -> RDD[U]:
        return MappedRDD(parents=(self,), function=function, num_of_partitions=self.num_of_partitions)

    def filter(self, predicate: Callable[[T], bool]) -> RDD[T]:
        NotImplementedError("Method not yet implemented.")

    def flatMap(self, f: Callable[[T], Iterable[U]]) -> RDD[U]:
        NotImplementedError("Method not yet implemented.")

    def collect(self) -> list[T]:
        NotImplementedError("Method not yet implemented.")

    def count(self) -> int:
        NotImplementedError("Method not yet implemented.")

    def reduce(self, f: Callable[[T, T], T]) -> T:
        NotImplementedError("Method not yet implemented.")

    def get_lineage_edges(self) -> list[tuple[str, str, str]]:
        visited = set()
        edges = []

        def dfw(rdd):
            visited.add(rdd)
            if rdd.parents:
                for parent in rdd.parents:
                    edges.append((parent.id, rdd.id, rdd.op))
                    if parent not in visited:
                        dfw(parent)

        dfw(rdd=self)
        return edges

    def get_ascii_lineage(self) -> str:
        visited = set()
        lines = []

        def dfw(rdd, depth):
            line = f"{'  '*depth}[{rdd.id[:16]}] {rdd.op} (partitions={rdd.num_of_partitions})"
            if rdd in visited:
                line += " (shared)"
                lines.append(line)
                return
            lines.append(line)
            visited.add(rdd)
            if rdd.parents:
                for parent in rdd.parents:
                    dfw(parent, depth+1)
                        
        dfw(rdd=self, depth=0)
        return "\n".join(lines)

class ParallelCollectionRDD(RDD):
    """Leaf RDD for holding in-memory data split deterministically into fixed partitions."""
    __slots__ = RDD.__slots__ + ('_partitions',)
    def __init__(self, data: Iterable[T], num_of_partitions: int):
        if not isinstance(data, Iterable):
            raise TypeError("ParallelCollectionRDD requires an iterable data object.")
        if not num_of_partitions >= 1:
            raise ValueError("Number of partitions attribute must be greater or equal to 1.")
        _partitions = self._create_partitions(data, num_of_partitions)
        object.__setattr__(self, '_partitions', _partitions)
        super().__init__(op="ParallelCollection", parents=(), num_of_partitions=num_of_partitions)

    @staticmethod
    def _create_partitions(data: Iterable[T], num_of_partitions: int) -> tuple[tuple[T, ...], ...]:
        partitions = [[] for _ in range(num_of_partitions)]
        for index, element in enumerate(data):
            partition_index = index % num_of_partitions
            partitions[partition_index].append(element)
        return tuple(tuple(partition) for partition in partitions)

    def compute(self, partition_index: int) -> Iterator[T]:
        if not (0 <= partition_index < self.num_of_partitions):
            raise AssertionError(f"Invalid partition index for RDD {self.id}.")
        return iter(self._partitions[partition_index])

class MappedRDD(RDD):
    """RDD node that represents a mapping operation."""
    __slots__ = RDD.__slots__ + ('function',)
    def __init__(self, parents: tuple, function: Callable[[T], U], num_of_partitions: int):
        if not isinstance(parents, tuple):
            raise AssertionError(f"Invalid 'parents' data type passed to RDD.")
        if len(parents) != 1:
            raise AssertionError("MappedRDD must have exactly one parent.")
        if not callable(function):
            raise TypeError(f"Invalid 'function' data type passed to RDD.")
        object.__setattr__(self, 'function', function)
        super().__init__(op="MappedRDD", parents=parents, num_of_partitions=num_of_partitions)
    
    def compute(self, partition_index: int) -> Iterator[U]:
        if not (0 <= partition_index < self.num_of_partitions):
            raise AssertionError(f"Invalid partition index for RDD {self.id}.")
        for x in self.parents[0].compute(partition_index):
            yield self.function(x)