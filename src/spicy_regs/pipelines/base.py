"""Base class for runnable end-to-end pipelines.

Subclass `Pipeline` to add a workflow that CI invokes. Each pipeline
wires sources and transforms together and exposes a single ``run()``
entry point.
"""

from abc import ABC, abstractmethod
from typing import ClassVar


class Pipeline(ABC):
    """A runnable end-to-end pipeline invoked by CI.

    Subclasses wire sources + transforms together. The CI runner addresses
    pipelines by their ``name`` class attribute and calls ``run()``.
    """

    name: ClassVar[str]

    @abstractmethod
    def run(self) -> None: ...
