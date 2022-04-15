"""
Objects/utility for standard things, which init with object creation or on a standard __enter__
"""
from __future__ import annotations

import threading
from typing import TypeVar, Generic, Type, Optional, List

from clearcut import get_logger

logger = get_logger(__name__)
ILP = TypeVar("ILP")


class InitLazyProxy(Generic[ILP]):
    """
    Delays __init__ of object until requested. Object is kept and future calls return the already init'ed object.

    No cleanup is tracked other than standard __del__ performed by Python.

    To use, at the module/top level:

        client: InitLazyProxy[Client] = InitLazyProxy(Client)(url='...', ...)

    Elsewhere in code, `client._` will be an inited and ready to use object:

        client.__.read(...) # if Client.read is an instance method.

    The args/kwargs passed in will be passed verbatim to Client.__init__. If no args/kwargs are required, that can
    be skipped:
        util: InitLazyProxy[UtilClass] = InitLazyProxy(UtilClass)

    TODO experiment with ParamSpec (would limit to 3.10+)
    """

    def __init__(self, proxied_type: Type[ILP]):
        self._lock: threading.Lock = threading.Lock()
        self._proxied_type: Type[ILP] = proxied_type
        self._inited: bool = False
        self._proxied_obj: Optional[ILP] = None
        self._args = tuple()
        self._kwargs = dict()

    def __call__(self, *args, **kwargs) -> InitLazyProxy[ILP]:
        self._args = args
        self._kwargs = kwargs
        return self

    @property
    def __(self) -> ILP:
        """Access to the underlying object. Object will be created the first time this is referenced."""
        if not self._inited:
            with self._lock:
                if not self._inited:
                    # Needs loading
                    logger.debug(f"Loading instance of {self._proxied_type}")
                    self._proxied_obj = self._proxied_type(*self._args, **self._kwargs)
                    self._inited = True

        return self._proxied_obj


CMLP = TypeVar("CMLP")


class ContextLazyProxy(Generic[CMLP]):
    """
    Delays __enter__ of object until requested. Object is kept in __enter__'ed state and future calls return the object
    ready to use.

    To use, created passed in the init'ed object at the module/top level:

        client: ContextLazyProxy[Client] = ContextLazyProxy(Client(url='...', ...))

    Then anywhere in code, can be used as a context manager:

        ...
        with client as c:
            c.read(...)

    `client.__enter__` will only be called the first time.

    An `__enter__`ed object can also be retrieved as with `InitLazyProxy`:

        ...
        client.__.read(...)

    Coupled with app shutdown, `ContextLazyProxy.cleanup()` to __exit__ all loaded objects.
    """

    _proxy_registry: List[ContextLazyProxy] = []
    """This is a global registry of created proxies. Is used to cleanup."""

    def __init__(self, obj: CMLP):
        self._lock: threading.Lock = threading.Lock()
        self._obj: CMLP = obj
        self._entered: bool = False

    def load_if_necessary(self) -> CMLP:
        if not self._entered:
            with self._lock:
                if not self._entered:
                    # Needs loading
                    if hasattr(self._obj, "__enter__"):
                        logger.debug(f"Loading {self._obj}")
                        self._obj.__enter__()
                    self._entered = True
                    ContextLazyProxy._proxy_registry.append(self)

        return self._obj

    def __enter__(self) -> CMLP:
        return self.load_if_necessary()

    def __exit__(self, exc_type, exc_val, exc_tb):
        # no-op because underlying __exit__ is called later
        pass

    @property
    def __(self) -> CMLP:
        return self.load_if_necessary()

    @classmethod
    def cleanup(cls, exc_type, exc_val, exc_tb):
        """Cleans up all ContextLazyProxy instances which have been instantiated."""
        # TODO call this automatically somehow?
        for proxy in cls._proxy_registry:
            if hasattr(proxy._obj, "__exit__"):
                logger.info(f"Cleaning up {proxy}")
                proxy._obj.__exit__(exc_type, exc_val, exc_tb)
