"""
Objects/utility for asyncio-based objects.
"""
import asyncio
from typing import TypeVar, Generic, List, Generator, Any

from clearcut import get_logger

logger = get_logger(__name__)

ALPT = TypeVar("ALPT")


class AIOLazyProxy(Generic[ALPT]):
    """
    Wraps a generic object, and only aenters it when necessary.

    To use, create passing in the init'ed object at the module/top level:

        client: AIOLazyProxy[Client] = AIOLazyProxy(Client(url='...', ...))

    Then anywhere in code, `await client` will return that object, appropriately instantiated with __aenter__. Can also be achieved by
    `async with client as c` (this async context manager won't kill the underlying object until the app closes).

    Coupled with app shutdown, `await AIOLazyProxy.cleanup()` to aexit all loaded objects.
    """

    _proxy_registry: List["AIOLazyProxy"] = []
    """This is a global registry of created proxies. Is used to cleanup."""

    def __init__(self, obj: ALPT):
        self._lock: asyncio.Lock = asyncio.Lock()
        self._obj: ALPT = obj
        self._aentered: bool = False

    async def load_if_necessary(self):
        async with self._lock:
            if not self._aentered:
                # Needs loading
                # TODO handle aexit...later?
                if hasattr(self._obj, "__aenter__"):
                    logger.info(f"Loading {self._obj}")
                    await self._obj.__aenter__()
                self._aentered = True
                AIOLazyProxy._proxy_registry.append(self)

        return self._obj

    @classmethod
    async def cleanup(cls, exc_type, exc_val, exc_tb):
        """Cleans up all AIOLazyProxy instances which have been instantiated."""
        # TODO call this automatically somehow?
        for proxy in cls._proxy_registry:
            if hasattr(proxy._obj, "__aexit__"):
                logger.info(f"Cleaning up {proxy}")
                await proxy._obj.__aexit__(exc_type, exc_val, exc_tb)

    def __await__(self) -> Generator[Any, None, ALPT]:
        return self.load_if_necessary().__await__()

    async def __aenter__(self):
        return await self.load_if_necessary()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass
