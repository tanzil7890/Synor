"""Writer-lock registry for one event loop and shared store facade."""

from __future__ import annotations

import asyncio
import threading
import weakref


_guard = threading.Lock()
_weak_locks: weakref.WeakKeyDictionary[object, _StateStoreWriterLock]
_strong_locks: dict[int, tuple[object, _StateStoreWriterLock]]
_weak_fences: weakref.WeakKeyDictionary[object, _StateStoreServingFence]
_strong_fences: dict[int, tuple[object, _StateStoreServingFence]]


class _StateStoreWriterLock:
    """Reusable async lock that rejects access from another event loop."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._event_loop: asyncio.AbstractEventLoop | None = None

    async def __aenter__(self) -> "_StateStoreWriterLock":
        event_loop = asyncio.get_running_loop()
        with _guard:
            if self._event_loop is None:
                self._event_loop = event_loop
            elif self._event_loop is not event_loop:
                raise RuntimeError(
                    "control-plane StateStore writers must share one event loop"
                )
        await self._lock.acquire()
        return self

    async def __aexit__(
        self,
        exc_type: object,
        exc_value: object,
        traceback: object,
    ) -> None:
        self._lock.release()


class _StateStoreServingFence:
    """Process-local emergency denials shared by one store facade."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._generations: dict[str, int] = {}

    def fail_closed(self, source_digest: str, generation: int) -> None:
        with self._lock:
            current = self._generations.get(source_digest, 0)
            if generation > current:
                self._generations[source_digest] = generation

    def clear_through(self, source_digest: str, generation: int) -> None:
        with self._lock:
            pending = self._generations.get(source_digest)
            if pending is not None and generation >= pending:
                del self._generations[source_digest]

    def pending_generation(self, source_digest: str) -> int | None:
        with self._lock:
            return self._generations.get(source_digest)

    def contains(self, source_digest: str) -> bool:
        with self._lock:
            return source_digest in self._generations


_weak_locks = weakref.WeakKeyDictionary()
_strong_locks = {}
_weak_fences = weakref.WeakKeyDictionary()
_strong_fences = {}


def state_store_writer_lock(store: object) -> _StateStoreWriterLock:
    """Return the shared writer lock for one store facade and event loop.

    Adapters over the same state must share the same ``StateStore`` object.
    Phase 2 requires all such adapters to run on one event loop. Distributed,
    multi-process, or cross-loop writers require a transactional backend.
    """

    with _guard:
        try:
            lock = _weak_locks.get(store)
        except TypeError:
            entry = _strong_locks.get(id(store))
            if entry is not None and entry[0] is store:
                return entry[1]
            lock = _StateStoreWriterLock()
            _strong_locks[id(store)] = (store, lock)
            return lock
        if lock is None:
            lock = _StateStoreWriterLock()
            _weak_locks[store] = lock
        return lock


def state_store_serving_fence(store: object) -> _StateStoreServingFence:
    """Return the process-local fail-closed overlay for one store facade."""

    with _guard:
        try:
            fence = _weak_fences.get(store)
        except TypeError:
            entry = _strong_fences.get(id(store))
            if entry is not None and entry[0] is store:
                return entry[1]
            fence = _StateStoreServingFence()
            _strong_fences[id(store)] = (store, fence)
            return fence
        if fence is None:
            fence = _StateStoreServingFence()
            _weak_fences[store] = fence
        return fence
