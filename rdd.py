from typing import Any, Callable, Iterable, Iterator, TypeVar
from __future__ import annotations

T = TypeVar("T")
U = TypeVar("U")

class RDD:
    __slots__ = ('id', 'op', 'parents', 'num_of_parts', '_locked')

    def __init__(self, id: str, op: str, parents: tuple, num_of_parts: int):
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
        pass

    def map(self, func: Callable[[T], U], name: str | None = None) -> RDD[U]:
        pass

    def filter(self, predicate: Callable[[T], bool], name: str | None = None) -> RDD[T]:
        pass

    def flatMap(self, f: Callable[[T], Iterable[U]], name: str | None = None) -> RDD[U]:
        pass

    def collect(self) -> list[T]:
        pass

    def count(self) -> int:
        pass

    def reduce(self, f: Callable[[T, T], T]) -> T:
        pass

    def lineage_edges(self) -> list[tuple[str, str, str]]:
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

    def print_lineage(self) -> str:
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