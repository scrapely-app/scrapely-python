"""Results a resumed run already saved are not saved or charged again.

A resurrected or rebooted run keeps its default dataset, but the
Actor's code starts again from the beginning. An Actor that keeps no state of
its own (no request queue) then collects the results it saved before once more,
and each charged push would bill them a second time.

On the first charged push to the default dataset of such a run, the SDK reads
the result key (``id`` unless the Actor sets another with
``Actor.set_result_key``) of the rows already saved. A later charged row whose
key is one of them is neither stored nor charged: the dataset already holds it
and it was paid for when it was first saved. It still counts as accepted, in
the push result and in ``get_charged_event_count``, so Actors that compare
what they pushed with what was charged do not read it as a refused charge.

The key must identify a row. When the saved rows repeat a key, or this run
pushes one key twice, it does not, and the run goes on as before: every row is
stored and charged. Nothing is skipped in a first run, in a run that saved
nothing yet, after a metamorph, or for pushes that are not charged per row.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union

logger = logging.getLogger('scrapely.charging')

DEFAULT_RESULT_KEY: Tuple[str, ...] = ('id',)

# Saved rows read at most (keys only, ~a few MB of memory). A larger dataset
# is left as it is: reading it would delay the run's first push too long.
MAX_SAVED_ROWS = 200_000
_PAGE = 5_000

_OFF_VALUES = {'0', 'off', 'false', 'no'}

KeyValue = Tuple[Any, ...]


def _normalize_key(key: Union[str, Sequence[str], None]) -> Optional[Tuple[str, ...]]:
    if key is None:
        return None
    if isinstance(key, str):
        names: Tuple[str, ...] = (key,)
    else:
        names = tuple(key)
    if not names or not all(isinstance(n, str) and n for n in names):
        raise ValueError('result key must be a field name or a list of field names')
    return names


def _scalar(value: Any) -> Any:
    """A key part: a non-empty string or an integer (bool, float, None, objects are not keys)."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip():
        return value
    return None


def row_key(row: Any, fields: Tuple[str, ...]) -> Optional[KeyValue]:
    """The row's key, or None when the row lacks a usable value for a key field."""
    if not isinstance(row, dict):
        return None
    parts = []
    for name in fields:
        part = _scalar(row.get(name))
        if part is None:
            return None
        # 123 and "123" are the same result.
        parts.append(str(part))
    return tuple(parts)


class ResumeDedupe:
    """Per-process state: which saved rows a resumed run may skip."""

    def __init__(self) -> None:
        self._fields: Optional[Tuple[str, ...]] = DEFAULT_RESULT_KEY
        self._state = 'unknown'  # unknown | on | off
        self._saved: Set[KeyValue] = set()
        self._seen: Set[KeyValue] = set()
        self._skipped = 0
        self._announced = False
        self._lock = asyncio.Lock()

    # Actor API
    def set_result_key(self, key: Union[str, Sequence[str], None]) -> None:
        fields = _normalize_key(key)
        if fields != self._fields:
            self._fields = fields
            # Keys read for the old fields are not valid for the new ones.
            if self._state == 'on':
                self._state = 'unknown'
                self._saved = set()
                self._seen = set()

    @property
    def skipped(self) -> int:
        return self._skipped

    @property
    def active(self) -> bool:
        return self._state == 'on'

    def _off(self, reason: str) -> None:
        if self._state == 'on':
            logger.info(
                'Results this run saved before it was resumed are charged again from here on: %s', reason
            )
        self._state = 'off'
        self._saved = set()
        self._seen = set()

    async def _load(self, manager: Any) -> None:
        """Decide once whether this run is a resumed one with saved rows."""
        if self._state != 'unknown':
            return
        self._state = 'off'
        if self._fields is None:
            return
        if (os.environ.get('SCRAPELY_RESUME_DEDUPE') or '').strip().lower() in _OFF_VALUES:
            return
        client = getattr(manager, '_client', None)
        run_id = getattr(manager, '_actor_run_id', None)
        from .charging import default_dataset_id_from_env

        dataset_id = default_dataset_id_from_env()
        if not getattr(manager, '_is_at_home', False) or client is None or not run_id or not dataset_id:
            return
        try:
            info = await client.get_json(f'/v2/datasets/{dataset_id}')
            item_count = int((info or {}).get('itemCount') or 0)
            if item_count <= 0:
                return  # first run, or nothing saved yet: nothing to skip
            run = await client.get_json(f'/v2/actor-runs/{run_id}')
            stats = (run or {}).get('stats') or {}
            resumed = sum(int(stats.get(k) or 0) for k in ('resurrectCount', 'rebootCount'))
            if resumed <= 0 or int(stats.get('metamorph') or 0) > 0:
                return
            if item_count > MAX_SAVED_ROWS:
                logger.info(
                    'This run was resumed with %s saved results, more than the %s the SDK reads to skip '
                    'results saved before; every result is saved and charged as it comes',
                    item_count,
                    MAX_SAVED_ROWS,
                )
                return
            saved: Set[KeyValue] = set()
            keyed = 0
            offset = 0
            fields_param = ','.join(self._fields)
            while offset < item_count:
                page = await client.get_json(
                    f'/v2/datasets/{dataset_id}/items',
                    params={'fields': fields_param, 'offset': offset, 'limit': _PAGE},
                )
                rows = page if isinstance(page, list) else (page or {}).get('items') or []
                if not rows:
                    break
                for row in rows:
                    key = row_key(row, self._fields)
                    if key is None:
                        continue
                    keyed += 1
                    if key in saved:
                        logger.debug('Saved rows repeat their result key; resumed-run dedupe is off')
                        return
                    saved.add(key)
                offset += len(rows)
            if keyed < 2:
                return
        except Exception as err:  # never fail a push over this
            logger.debug('Could not read the saved results of the resumed run: %s', err)
            return
        self._saved = saved
        self._seen = set()
        self._state = 'on'

    async def ensure(self, manager: Any) -> bool:
        if self._state == 'unknown':
            async with self._lock:
                await self._load(manager)
        return self._state == 'on'

    @property
    def lock(self) -> asyncio.Lock:
        return self._lock

    def plan(self, rows: List[Any]) -> Tuple[List[Any], List[KeyValue], List[Optional[KeyValue]]]:
        """Split rows into (rows to push, keys of skipped rows, key of each row to push or None).

        Nothing is recorded until ``commit``; the caller holds ``lock``.
        """
        if self._state != 'on' or self._fields is None:
            return rows, [], []
        kept: List[Any] = []
        skipped: List[KeyValue] = []
        pushed: List[Optional[KeyValue]] = []
        batch: Set[KeyValue] = set()
        for row in rows:
            key = row_key(row, self._fields)
            if key is None:
                kept.append(row)
                pushed.append(None)
                continue
            if key in self._seen or key in batch:
                # This run pushes one key twice: the key does not identify a row,
                # so nothing of this push is skipped either.
                self._off('the result key repeats in this run')
                return rows, [], []
            batch.add(key)
            if key in self._saved:
                skipped.append(key)
            else:
                kept.append(row)
                pushed.append(key)
        return kept, skipped, pushed

    def commit(self, skipped: Iterable[KeyValue], pushed: Iterable[Optional[KeyValue]]) -> int:
        """Record a push that went through: ``pushed`` = keys of the rows it stored."""
        n = 0
        for key in skipped:
            self._saved.discard(key)
            self._seen.add(key)
            n += 1
        for key in pushed:
            if key is not None:
                self._seen.add(key)
        if n:
            self._skipped += n
            if not self._announced:
                self._announced = True
                logger.info(
                    'This run was resumed: results it saved before (same %s) are not saved or charged again',
                    ' + '.join(repr(f) for f in (self._fields or ())),
                )
        return n


_instance: Optional[ResumeDedupe] = None


def get_resume_dedupe() -> ResumeDedupe:
    global _instance
    if _instance is None:
        _instance = ResumeDedupe()
    return _instance


def reset_resume_dedupe() -> None:
    """Tests: forget the per-process state."""
    global _instance
    _instance = None
