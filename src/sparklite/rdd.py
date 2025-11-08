from __future__ import annotations
from typing import Any, Callable, Iterable, Iterator, TypeVar

T = TypeVar("T")
U = TypeVar("U")

class RDD:
    __slots__ = ('id', 'op', 'parents', 'num_of_parts', '_locked')

    def __init__(self, id: str, op: str, parents: tuple, num_of_parts: int):
        if not isinstance(parents, tuple):
            raise ValueError("Parents attribute must be a tuple.")
        if not num_of_parts >= 1:
            raise ValueError("Number of partitions attribute must be greater or equal to 1.")

        object.__setattr__(self, 'id', id)
        object.__setattr__(self, 'op', op)
        object.__setattr__(self, 'parents', parents)
        object.__setattr__(self, 'num_of_parts', num_of_parts)
        object.__setattr__(self, '_locked', True)

    def __setattr__(self, key, value):
        if getattr(self, '_locked', False):
            raise AttributeError("An RDD is an immutable object.")
        object.__setattr__(self, key, value)

    def compute(self, part_index: int) -> Iterator[Any]:
        NotImplementedError("Method not yet implemented.")

    def map(self, func: Callable[[T], U], name: str | None = None) -> RDD[U]:
        NotImplementedError("Method not yet implemented.")

    def filter(self, predicate: Callable[[T], bool], name: str | None = None) -> RDD[T]:
        NotImplementedError("Method not yet implemented.")

    def flatMap(self, f: Callable[[T], Iterable[U]], name: str | None = None) -> RDD[U]:
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
            line = f"{'  '*depth}[{rdd.id[:8]}] {rdd.op} (parts={rdd.num_of_parts})"
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