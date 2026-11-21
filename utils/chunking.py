from itertools import islice
from typing import Iterable, Iterator, TypeVar

T = TypeVar("T")


def chunked(iterable: Iterable[T], size: int) -> Iterator[list[T]]:
    """
    Group an iterable into lists of at most `size` items, preserving order.
    The final chunk may be smaller than `size`.
    """
    iterator = iter(iterable)
    while True:
        chunk = list(islice(iterator, size))
        if not chunk:
            return
        yield chunk
