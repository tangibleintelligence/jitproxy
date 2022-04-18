"""
Objects/utility for asyncio-based objects.
"""
from __future__ import annotations

import asyncio
import threading
import warnings
from typing import TypeVar, Generic, List, Generator, Any, Type, Union

from clearcut import get_logger

logger = get_logger(__name__)

LPT = TypeVar("LPT")


class BaseLazyProxy(Generic[LPT]):
    """
    Base of both standard and aio lazy proxies.
    """

    _proxy_registry: List[BaseLazyProxy] = []
    """This is a global registry of created proxies. Is used to cleanup."""

    _cleanup_lock: threading.Lock = threading.Lock()

    def __init__(self, obj_or_class: Union[LPT, Type[LPT]]):
        """
        Pass in an init'ed object, or a type, to delegate it to the lazy proxy. If a type is passed in, both __init__ and enter/aenter
        will be delayed.
        """

        # self._enter_lock: threading.Lock = asyncio.Lock()
        self._init_lock: threading.Lock = threading.Lock()

        self._entered: bool = False
        BaseLazyProxy._proxy_registry.append(self)

        if isinstance(obj_or_class, type):
            self._proxied_type: Type[LPT] = obj_or_class
            self._proxied_obj: LPT = None
            self._inited: bool = False
            self._args = tuple()
            self._kwargs = dict()
        else:
            self._proxied_type: Type[LPT] = type(obj_or_class)
            self._proxied_obj: LPT = obj_or_class
            self._inited: bool = True
            self._args = None
            self._kwargs = None

    def __call__(self, *args, **kwargs):
        if self._inited:
            warnings.warn("Object already init'ed. args/kwargs will have no effect")
            return self
        else:
            self._args = args
            self._kwargs = kwargs

            return self

    def _init_if_necessary(self) -> LPT:
        if not self._inited:
            with self._init_lock:
                if not self._inited:
                    # Needs init'ing
                    logger.debug(f"Init'ing new instance of {self._proxied_type.__qualname__}")
                    self._proxied_obj = self._proxied_type(*self._args, **self._kwargs)
                    self._inited = True

        return self._proxied_obj

    @property
    def unmanaged_object(self) -> LPT:
        """Init'ed object that may or may not be within its enter/exit lifecycle. Generally should not be used except for internal util."""
        return self._init_if_necessary()

    @classmethod
    def cleanup_sync(cls, exc_type, exc_val, exc_tb):
        """
        Cleans up all LazyProxy instances which have been instantiated.
        """
        with cls._cleanup_lock:
            for proxy in cls._proxy_registry:
                if proxy._inited and proxy._entered:
                    if hasattr(proxy.unmanaged_object, "__aexit__"):
                        logger.warning(f"Unable to clean up aio proxied: {repr(proxy.unmanaged_object)}")
                    elif hasattr(proxy.unmanaged_object, "__exit__"):
                        logger.debug(f"Cleaning up {repr(proxy.unmanaged_object)}")
                        proxy.unmanaged_object.__exit__(exc_type, exc_val, exc_tb)

    @classmethod
    async def cleanup(cls, exc_type, exc_val, exc_tb):
        """
        Cleans up all LazyProxy instances which have been instantiated.
        """
        # TODO call this automatically somehow?
        with cls._cleanup_lock:
            for proxy in cls._proxy_registry:
                if proxy._inited and proxy._entered:
                    if hasattr(proxy.unmanaged_object, "__aexit__"):
                        logger.debug(f"Cleaning up {repr(proxy.unmanaged_object)}")
                        await proxy.unmanaged_object.__aexit__(exc_type, exc_val, exc_tb)
                    elif hasattr(proxy.unmanaged_object, "__exit__"):
                        logger.debug(f"Cleaning up {repr(proxy.unmanaged_object)}")
                        proxy.unmanaged_object.__exit__(exc_type, exc_val, exc_tb)


class AIOLazyProxy(BaseLazyProxy, Generic[LPT]):
    """
    Wraps an object, delaying its init and aenter calls until necessary.

    Use example: at the top-level of a module:

        # Only delay aenter
        client: AIOLazyProxy[Client] = AIOLazyProxy(Client(url='...', ...))

        # Delay init and aenter
        client: AIOLazyProxy[Client] = AIOLazyProxy(Client)(url='...', ...)

    Then anywhere in code, `await client` will return that object, appropriately instantiated and within its lifecycle. Can also be achieved
    by `async with client as c` (this async context manager won't kill the underlying object until the app closes).

    Coupled with app shutdown, `await AIOLazyProxy.cleanup()` to cleanup all loaded objects.
    """

    def __init__(self, obj_or_class: Union[LPT, Type[LPT]]):
        super().__init__(obj_or_class)
        self._aenter_lock: asyncio.Lock = asyncio.Lock()

    async def _aenter_if_necessary(self):
        if not self._entered:
            async with self._aenter_lock:
                if not self._entered:
                    # Needs loading
                    # TODO handle aexit...later?
                    if hasattr(self.unmanaged_object, "__aenter__"):
                        logger.debug(f"Loading {repr(self.unmanaged_object)}")
                        await self.unmanaged_object.__aenter__()
                    self._entered = True

        return self.unmanaged_object

    def __await__(self) -> Generator[Any, None, LPT]:
        return self._aenter_if_necessary().__await__()

    async def __aenter__(self):
        return await self._aenter_if_necessary()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class StandardLazyProxy(BaseLazyProxy, Generic[LPT]):
    """
    Wraps an object, delaying its init and enter calls until necessary.

    Use example: at the top-level of a module:

        # Only delay enter
        client: AIOLazyProxy[Client] = AIOLazyProxy(Client(url='...', ...))

        # Delay init and enter
        client: AIOLazyProxy[Client] = AIOLazyProxy(Client)(url='...', ...)

    Then anywhere in code, `client.__` will return that object, appropriately instantiated and within its lifecycle. Can also be achieved
    by `with client as c` (this sync context manager won't kill the underlying object until the app closes).

    Coupled with app shutdown, `await StandardLazyProxy.cleanup()` to aexit all loaded objects.
    """

    def __init__(self, obj_or_class: Union[LPT, Type[LPT]]):
        super().__init__(obj_or_class)
        self._enter_lock: threading.Lock = threading.Lock()

    def _enter_if_necessary(self) -> LPT:
        if not self._entered:
            with self._enter_lock:
                if not self._entered:
                    # Needs loading
                    if hasattr(self.unmanaged_object, "__enter__"):
                        logger.debug(f"Loading {repr(self.unmanaged_object)}")
                        self.unmanaged_object.__enter__()
                    self._entered = True

        return self.unmanaged_object

    @property
    def __(self) -> LPT:
        return self._enter_if_necessary()

    def __enter__(self) -> LPT:
        return self._enter_if_necessary()

    def __exit__(self, exc_type, exc_val, exc_tb):
        # no-op because underlying __exit__ is called later
        pass
