"""
Objects/utility for asyncio-based objects.
"""
from __future__ import annotations

import asyncio
import threading
import warnings
from types import SimpleNamespace
from typing import TypeVar, Generic, List, Generator, Any, Type, Union, Optional

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

    def __init__(self, obj_or_class: Union[LPT, Type[LPT]], thread_local: Optional[bool] = None):
        """
        Pass in an init'ed object, or a type, to delegate it to the lazy proxy. If a type is passed in, both __init__ and enter/aenter
        will be delayed.
        """

        # self._enter_lock: threading.Lock = asyncio.Lock()
        logger.debug(f"Making baselazyproxy for {obj_or_class} in thread {threading.current_thread().name}")
        self._init_lock: threading.Lock = threading.Lock()

        if thread_local is None:
            if isinstance(obj_or_class, type):
                thread_local = True
            else:
                thread_local = False
        self._thread_local: bool = thread_local

        self._thread_local_storage: threading.local = threading.local()
        self._global_storage = SimpleNamespace()

        BaseLazyProxy._proxy_registry.append(self)

        if isinstance(obj_or_class, type):
            logger.debug(f"Making baselazyproxy for type {obj_or_class} in thread {threading.current_thread().name}")
            self._proxied_type: Type[LPT] = obj_or_class
            self._store_obj(None)
            self._store_inited(False)
            self._store_entered(False)
            self._args = tuple()
            self._kwargs = dict()
        else:
            self._proxied_type: Type[LPT] = type(obj_or_class)
            self._store_obj(obj_or_class)
            self._store_inited(True)
            self._store_entered(False)
            self._args = None
            self._kwargs = None

    def __call__(self, *args, **kwargs):
        if self._is_inited():
            warnings.warn("Object already init'ed. args/kwargs will have no effect")
            return self
        else:
            self._args = args
            self._kwargs = kwargs

            return self

    def _init_if_necessary(self) -> LPT:
        logger.debug(f"init'ing if necessary instance of {self._proxied_type.__qualname__} in thread {threading.current_thread().name}")
        if not self._is_inited():
            with self._init_lock:
                if not self._is_inited():
                    # Needs init'ing
                    logger.debug(f"Init'ing new instance of {self._proxied_type.__qualname__} in thread {threading.current_thread().name}")
                    # Replace any args or kwargs that are proxies with their proxied objects. We'll enter/aenter later.
                    args = [arg.unmanaged_object if isinstance(arg, BaseLazyProxy) else arg for arg in self._args]
                    kwargs = {k: arg.unmanaged_object if isinstance(arg, BaseLazyProxy) else arg for k, arg in self._kwargs.items()}

                    self._store_obj(self._proxied_type(*args, **kwargs))
                    self._store_inited(True)
                else:
                    logger.debug(
                        f"Not init'ing new instance of {self._proxied_type.__qualname__} in thread {threading.current_thread().name}"
                    )

        return self._get_obj()

    def _correct_storage(self) -> Union[SimpleNamespace, threading.local]:
        if self._thread_local:
            return self._thread_local_storage
        else:
            return self._global_storage

    def _store_obj(self, obj: LPT):
        self._correct_storage().obj = obj

    def _get_obj(self) -> LPT:
        return self._correct_storage().obj

    def _is_inited(self) -> bool:
        return getattr(self._correct_storage(), "inited", False)

    def _store_inited(self, inited: bool):
        self._correct_storage().inited = inited

    def _is_entered(self) -> bool:
        return getattr(self._correct_storage(), "entered", False)

    def _store_entered(self, entered: bool):
        self._correct_storage().entered = entered

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
                if proxy._is_inited() and proxy._is_entered():
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
                if proxy._is_inited() and proxy._is_entered():
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

    def __init__(self, obj_or_class: Union[LPT, Type[LPT]], thread_local: Optional[bool] = None):
        super().__init__(obj_or_class, thread_local)
        logger.debug(
            f"creating aenter lock in thread {threading.current_thread().name} with loop {asyncio.get_event_loop()} / {id(asyncio.get_event_loop())}"
        )
        self._aenter_lock: asyncio.Lock = asyncio.Lock()

    async def _aenter_if_necessary(self):
        if not self._is_entered():
            logger.debug(
                f"checking aenter lock in thread {threading.current_thread().name} / {id(threading.current_thread())} with loop {self._aenter_lock._loop} / {id(self._aenter_lock._loop)} of {self._proxied_type.__qualname__} in thread {threading.current_thread().name} with loop {asyncio.get_event_loop()} / {id(asyncio.get_event_loop())}"
            )
            async with self._aenter_lock:
                if not self._is_entered():
                    logger.debug(
                        f"aenter'ing new instance of {self._proxied_type.__qualname__} in thread {threading.current_thread().name}"
                    )
                    # Needs loading
                    # First enter any deps
                    for arg in self._args:
                        if isinstance(arg, StandardLazyProxy):
                            # noinspection PyProtectedMember
                            arg._enter_if_necessary()
                        elif isinstance(arg, AIOLazyProxy):
                            await arg._aenter_if_necessary()

                    for arg in self._kwargs.values():
                        if isinstance(arg, StandardLazyProxy):
                            # noinspection PyProtectedMember
                            arg._enter_if_necessary()
                        elif isinstance(arg, AIOLazyProxy):
                            await arg._aenter_if_necessary()

                    # Then enter this one
                    if hasattr(self.unmanaged_object, "__aenter__"):
                        logger.debug(f"Loading {repr(self.unmanaged_object)}")
                        await self.unmanaged_object.__aenter__()
                    self._store_entered(True)

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

    def __init__(self, obj_or_class: Union[LPT, Type[LPT]], thread_local: Optional[bool] = None):
        super().__init__(obj_or_class, thread_local)
        self._enter_lock: threading.Lock = threading.Lock()

    def _enter_if_necessary(self) -> LPT:
        if not self._is_entered():
            with self._enter_lock:
                if not self._is_entered():
                    logger.debug(f"enter'ing new instance of {self._proxied_type.__qualname__} in thread {threading.current_thread().name}")
                    # Needs loading
                    # First enter any deps
                    for arg in self._args:
                        if isinstance(arg, StandardLazyProxy):
                            arg._enter_if_necessary()

                    for arg in self._kwargs.values():
                        if isinstance(arg, StandardLazyProxy):
                            arg._enter_if_necessary()

                    # Then enter this one
                    if hasattr(self.unmanaged_object, "__enter__"):
                        logger.debug(f"Loading {repr(self.unmanaged_object)}")
                        self.unmanaged_object.__enter__()
                    self._store_entered(True)

        return self.unmanaged_object

    @property
    def __(self) -> LPT:
        return self._enter_if_necessary()

    def __enter__(self) -> LPT:
        return self._enter_if_necessary()

    def __exit__(self, exc_type, exc_val, exc_tb):
        # no-op because underlying __exit__ is called later
        pass
