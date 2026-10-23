"""
Main Actor class for the Scrapely Python SDK.

The core class for building Actors on the Scrapely platform.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import signal
import sys
import time
import warnings
from collections.abc import MutableMapping
from datetime import datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple, Union, cast

from .charging import (
    ChargingManager,
    is_default_dataset_item_event,
    push_data_and_charge,
    push_data_with_charges,
    set_active_charging_manager,
)
from .configuration import Configuration
from .crypto import decrypt_secret_inputs
from .proxy_configuration import with_proxy_key_aliases
from .charging_client import create_charging_client
from .errors import ForbiddenError
from .storage.api_client import ApiClient, ApiClientError
from .storage.client import is_storage_uuid
from .types import (
    AbortOptions,
    ActorRun,
    ApifyEnv,
    CallOptions,
    ChargeResult,
    ExitOptions,
    InitOptions,
    MetamorphOptions,
    OpenStorageOptions,
    RebootOptions,
    SetStatusMessageOptions,
    StartOptions,
    WebhookOptions,
)
from .webhook import Webhook

if TYPE_CHECKING:
    from crawlee.events import EventManager
    from crawlee.storage_clients._base import StorageClient
    from crawlee.storages import Dataset, KeyValueStore, RequestQueue
    from scrapely_client import ApifyClientAsync

    from .events import Event
    from .storage._metadata import StorageType
    from .storage.scrapely_storage_client import ScrapelyStorageClient
    from .storage.smart_scrapely_storage_client import SmartScrapelyStorageClient

    from .browser_configuration import BrowserConfiguration
    from .proxy_configuration import ProxyConfiguration

logger = logging.getLogger(__name__)

# Names that need Crawlee, resolved on first use instead of at import: importing
# Crawlee costs ~1 s per run on the platform. They stay module attributes
# (``scrapely.actor.Dataset`` works and can be monkeypatched): ``_lazy`` returns
# the module-global value when one is set, else imports and caches it.
_LAZY_ATTRS: Dict[str, Tuple[str, str]] = {
    'CrawleeConfiguration': ('crawlee.configuration', 'Configuration'),
    'StorageClient': ('crawlee.storage_clients._base', 'StorageClient'),
    'Dataset': ('crawlee.storages', 'Dataset'),
    'KeyValueStore': ('crawlee.storages', 'KeyValueStore'),
    'RequestQueue': ('crawlee.storages', 'RequestQueue'),
    'EventManager': ('crawlee.events', 'EventManager'),
    'Event': ('scrapely.events', 'Event'),
    'LocalEventManager': ('scrapely.events', 'LocalEventManager'),
    'ScrapelyEventManager': ('scrapely.events', 'ScrapelyEventManager'),
    'StorageType': ('scrapely.storage._metadata', 'StorageType'),
    'resolve_alias_from_storages_json': ('scrapely.storage._metadata', 'resolve_alias_from_storages_json'),
    'disconnect_storage': ('scrapely.storage.patch_storage', 'disconnect_storage'),
    'get_api_storage_factory': ('scrapely.storage.patch_storage', 'get_api_storage_factory'),
    'get_smart_storage_client': ('scrapely.storage.patch_storage', 'get_smart_storage_client'),
    'set_api_storage': ('scrapely.storage.patch_storage', 'set_api_storage'),
    'set_storage_client': ('scrapely.storage.patch_storage', 'set_storage_client'),
    'ScrapelyStorageClient': ('scrapely.storage.scrapely_storage_client', 'ScrapelyStorageClient'),
    'SmartScrapelyStorageClient': (
        'scrapely.storage.smart_scrapely_storage_client',
        'SmartScrapelyStorageClient',
    ),
    '_configure_logging': ('scrapely.log', '_configure_logging'),
}


def _lazy(name: str) -> Any:
    """Return a Crawlee-dependent module attribute, importing it on first use."""
    value = globals().get(name, _MISSING)
    if value is _MISSING:
        import importlib

        module_name, attr = _LAZY_ATTRS[name]
        value = getattr(importlib.import_module(module_name), attr)
        globals()[name] = value
    return value


_MISSING = object()


def __getattr__(name: str) -> Any:
    if name in _LAZY_ATTRS:
        return _lazy(name)
    raise AttributeError(f'module {__name__!r} has no attribute {name!r}')

# Terminal run statuses used by the run-control polling loops (call/call_task).
_RUN_TERMINAL_STATUSES = frozenset({'SUCCEEDED', 'FAILED', 'ABORTED', 'TIMED-OUT'})


def _payload_dict(payload: Any) -> Dict[str, Any]:
    """Normalize an unwrapped API envelope payload to a plain dict.

    ``ApiClient.get_json``/``post_json``/``put_json`` return the unwrapped
    ``data`` field, which may be a non-dict (``{}`` fallback, list, scalar)
    when the endpoint responds with an empty or non-JSON body.
    """
    if isinstance(payload, dict):
        return cast(Dict[str, Any], payload)
    return {}


def _timeout_seconds(value: Union[int, str, None]) -> Optional[int]:
    """Normalize a ``CallOptions.timeout`` value to poll deadline seconds.

    Accepts a positive int; ``'inherit'``, ``None`` and non-positive values
    mean "no client-side deadline" (the platform's own run timeout applies).
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int) and value > 0:
        return value
    return None


def _resolve_run_timeout(value: Union[int, str, None]) -> Optional[int]:
    """Resolve a ``StartOptions``/``CallOptions`` timeout to wire seconds.

    ``'inherit'`` resolves to the current run's remaining seconds (Node SDK
    ``resolveCallTimeout`` parity, warning when no platform timeout is set);
    positive ints pass through; everything else means "not sent" so the
    platform default applies.
    """
    if isinstance(value, str) and value.lower() == 'inherit':
        remaining_ms = Actor.get_remaining_time()
        if remaining_ms is None:
            logger.warning(
                "Using 'inherit' for timeout is only possible when running on the platform "
                'and when the timeout for the Actor run is set (ACTOR_TIMEOUT_AT).'
            )
            return None
        return max(0, -(-remaining_ms // 1000))  # ceil(ms → secs)
    return _timeout_seconds(value)


def _webhook_body(webhook: Any) -> Dict[str, Any]:
    """Serialize a Webhook/WebhookOptions-like object to the API body format."""
    to_dict = getattr(webhook, 'to_dict', None)
    if callable(to_dict):
        return cast(Dict[str, Any], to_dict())

    body: Dict[str, Any] = {
        'eventTypes': [getattr(e, 'value', e) for e in (webhook.event_types or [])],
        'requestUrl': webhook.request_url,
    }
    if getattr(webhook, 'payload_template', None):
        body['payloadTemplate'] = webhook.payload_template
    if getattr(webhook, 'headers_template', None):
        body['headersTemplate'] = webhook.headers_template
    if getattr(webhook, 'idempotency_key', None):
        body['idempotencyKey'] = webhook.idempotency_key
    description = getattr(webhook, 'description', None)
    if description:
        body['description'] = description
    ignore_ssl_errors = getattr(webhook, 'ignore_ssl_errors', None)
    if ignore_ssl_errors is not None:
        body['ignoreSslErrors'] = bool(ignore_ssl_errors)
    do_not_retry = getattr(webhook, 'do_not_retry', None)
    if do_not_retry is not None:
        body['doNotRetry'] = bool(do_not_retry)
    return body


def _run_options_params(
    *,
    build: Optional[str] = None,
    timeout: Union[int, str, None] = None,
    memory_mbytes: Optional[int] = None,
    max_total_charge_usd: Optional[float] = None,
    restart_on_error: Optional[bool] = None,
    force_permission_level: Optional[Any] = None,
    webhooks: Optional[List[Any]] = None,
) -> Dict[str, Any]:
    """Build the run-option query parameters for run-start requests.

    Server wire contract: the POST body of a run-start request is the run
    input itself; run options (build/timeout/memory/maxTotalChargeUsd/
    restartOnError/forcePermissionLevel) are read from query parameters only.
    Inline webhooks ride along base64-encoded in the ``webhooks`` query param
    (the query-param transport the server supports alongside the body one).
    """
    params: Dict[str, Any] = {}
    if build:
        params['build'] = build
    timeout_secs = _resolve_run_timeout(timeout)
    if timeout_secs is not None:
        params['timeout'] = timeout_secs
    if memory_mbytes:
        params['memory'] = memory_mbytes
    if max_total_charge_usd is not None:
        params['maxTotalChargeUsd'] = max_total_charge_usd
    if restart_on_error is not None:
        params['restartOnError'] = 'true' if restart_on_error else 'false'
    if force_permission_level is not None:
        level = getattr(force_permission_level, 'value', force_permission_level)
        params['forcePermissionLevel'] = str(level)
    if webhooks:
        encoded = base64.b64encode(
            json.dumps([_webhook_body(w) for w in webhooks]).encode('utf-8'),
        ).decode('ascii')
        params['webhooks'] = encoded
    return params


def _is_api_storage_config(storage: Any) -> bool:
    return (
        isinstance(storage, dict)
        and 'api_url' in storage
        and 'token' in storage
        and 'run_id' in storage
    )


class _ClassProperty:
    """Read-only class property descriptor."""

    def __init__(self, func: Callable[..., Any]) -> None:
        self.func = func

    def __get__(self, obj: Any, owner: Any = None) -> Any:
        return self.func(owner)


def _system_exit_code(exc: Any) -> int:
    """Process exit code carried by a SystemExit (sys.exit semantics)."""
    code = getattr(exc, 'code', None)
    if code is None:
        return 0
    return code if isinstance(code, int) else 1


class _ActorMeta(type):
    """Metaclass enabling async with Actor: by delegating to classmethods."""

    # Declarations for the Actor classmethods delegated to below (typing only).
    init: Callable[..., Any]
    _shutdown: Callable[..., Any]
    _exit_code: int

    async def __aenter__(cls) -> Any:
        await cls.init()
        return cls

    async def __aexit__(cls, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_type is not None and issubclass(exc_type, SystemExit):
            # Actor.exit()/fail()/sys.exit() inside `async with Actor:` is a deliberate
            # exit, not a failure (Apify parity): keep its exit code, log nothing.
            cls._exit_code = _system_exit_code(exc_val)
        elif exc_type is not None:
            cls._exit_code = 1
            logger.error(f'Actor failed: {exc_val}')
        await cls._shutdown()



def _element_markup(obj: Any) -> Optional[str]:
    """The markup of a parsed XML/HTML element, or None when ``obj`` is not one.

    Debug captures store the source an item was read from. Feed and page parsers hand over elements
    (xml.etree / lxml elements and trees, BeautifulSoup tags, selectolax nodes, parsel / Scrapy
    selectors); their ``str()`` is only a repr such as ``<Element 'change' at 0x...>``, which shows
    nothing of the source. Optional parsers are recognised by module, never imported here.
    """
    import xml.etree.ElementTree as ET

    if isinstance(obj, ET.ElementTree):
        root = obj.getroot()
        return ET.tostring(root, encoding='unicode') if root is not None else ''
    if isinstance(obj, ET.Element):
        if obj.tail:  # the text after the element belongs to its parent, as with lxml's with_tail=False
            import copy

            obj = copy.copy(obj)
            obj.tail = None
        return ET.tostring(obj, encoding='unicode')
    modules = [getattr(c, '__module__', '') or '' for c in type(obj).__mro__]
    if any(m == 'lxml' or m.startswith('lxml.') for m in modules):
        from lxml import etree  # type: ignore[import-not-found]

        out = etree.tostring(obj, encoding='unicode', with_tail=False)
        return out if isinstance(out, str) else None
    if any(m == 'bs4' or m.startswith('bs4.') for m in modules):
        return str(obj)
    if any(m == 'selectolax' or m.startswith('selectolax.') for m in modules):
        html = getattr(obj, 'html', None)
        return html if isinstance(html, str) else None
    if any(m == 'parsel' or m.startswith('parsel.') for m in modules) and callable(getattr(obj, 'get', None)):
        out = obj.get()
        return out if isinstance(out, str) else None
    return None


def _capture_json_default(obj: Any) -> Any:
    """json.dumps fallback for debug captures: elements as their markup, anything else as str()."""
    try:
        markup = _element_markup(obj)
    except Exception:  # an unusual node type must not lose the rest of the payload
        markup = None
    return markup if markup is not None else str(obj)


class Actor(metaclass=_ActorMeta):
    """Core class for building Actors on the Scrapely platform."""

    _instance: Optional['Actor'] = None
    _initialized: bool = False
    _config: Optional[Configuration] = None
    _client: Optional[ApiClient] = None
    _apify_client_instance: Optional['ApifyClientAsync'] = None
    _scrapely_storage: Optional[ScrapelyStorageClient] = None
    _smart_storage: Optional[SmartScrapelyStorageClient] = None
    _default_dataset: Optional[Dataset] = None
    _default_kvs: Optional[KeyValueStore] = None
    _default_rq: Optional[RequestQueue] = None
    _event_manager: Optional[EventManager] = None
    _cached_input: Optional[Dict[str, Any]] = None
    _exit_code: int = 0
    _status_message: Optional[str] = None
    _charging_manager: Optional[ChargingManager] = None
    _use_state_stores: set = set()
    # Lifecycle bookkeeping set in init() / cleared in _shutdown().
    _lifecycle_config_snapshot: Optional[Dict[str, Any]] = None
    _signal_shutdown_started: bool = False

    def __init__(
        self,
        options: Optional[Dict[str, Any]] = None,
        *,
        configure_logging: bool = True,
        exit_code: int = 0,
        status_message: Optional[str] = None,
        cleanup_timeout: timedelta = timedelta(seconds=30),
        exit_process: Optional[bool] = None,
        event_listeners_timeout: Optional[timedelta] = None,
    ):
        """Support ``async with Actor(configure_logging=False):`` as well as ``async with Actor:``.

        ``options`` is a configuration dict. The context
        manager never calls ``sys.exit`` (``exit_process`` is accepted for signature parity),
        and ``event_listeners_timeout`` is accepted but not applied.
        """
        self._options = options or {}
        self._configure_logging = configure_logging
        self._instance_exit_code = exit_code
        self._instance_status_message = status_message
        self._cleanup_timeout = cleanup_timeout

    async def __aenter__(self) -> 'Actor':
        await type(self).init(
            InitOptions(configuration=self._options or None, configure_logging=self._configure_logging)
        )
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        cls = type(self)
        if exc_type is not None and issubclass(exc_type, SystemExit):
            # Deliberate exit via Actor.exit()/fail()/sys.exit(), not a failure.
            cls._exit_code = _system_exit_code(exc_val)
        elif exc_type is not None:
            cls._exit_code = 1
            logger.error(f'Actor failed: {exc_val}')
        else:
            cls._exit_code = self._instance_exit_code
        if self._instance_status_message:
            cls._status_message = self._instance_status_message
            await cls._persist_terminal_status_message(self._instance_status_message)
        await cls._shutdown(timeout_secs=int(self._cleanup_timeout.total_seconds()))

    @classmethod
    def get_default_instance(cls) -> 'Actor':
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    async def init(cls, options: Optional[InitOptions] = None) -> None:
        cls._main_loop = asyncio.get_running_loop()  # for debug_capture_sync from worker threads
        if cls._initialized:
            return

        if options is None or options.configure_logging:
            _lazy('_configure_logging')()

        # Do NOT set _initialized until everything succeeds (failed init must be retryable).
        cls._config = Configuration.get_global_config()
        # Snapshot keys we may overwrite so shutdown can restore prior lifecycle state.
        cls._lifecycle_config_snapshot = {
            'api_base_url': cls._config.get('api_base_url'),
            'token': cls._config.get('token'),
            'actor_run_id': cls._config.get('actor_run_id'),
        }
        if options and options.configuration:
            for key, value in options.configuration.items():
                cls._config.set(key, value)

        crawlee_config = _lazy('CrawleeConfiguration').get_global_configuration()
        if cls._config.get('is_at_home'):
            crawlee_config.purge_on_start = False

        storage_opt = options.storage if options else None

        try:
            # Apply explicit storage credentials BEFORE event manager / charging so
            # WebSocket + PPE bill the same run as storage.
            if storage_opt is not None and _is_api_storage_config(storage_opt):
                if storage_opt.get('api_url'):
                    cls._config.set('api_base_url', storage_opt['api_url'])
                if storage_opt.get('token'):
                    cls._config.set('token', storage_opt['token'])
                if storage_opt.get('run_id'):
                    cls._config.set('actor_run_id', storage_opt['run_id'])

            if storage_opt is not None:
                if _is_api_storage_config(storage_opt):
                    logger.info(
                        '[Scrapely] Initializing API storage for run %s',
                        storage_opt['run_id'],
                    )
                    smart = _lazy('set_api_storage')(storage_opt)
                elif isinstance(storage_opt, _lazy('StorageClient')):
                    smart = _lazy('set_storage_client')(storage_opt)
                else:
                    raise TypeError(
                        'Actor.init() storage option must be ApiStorageConfig or StorageClient'
                    )
            else:
                api_url = cls._config.get('api_base_url')
                token = cls._config.get('token')
                run_id = cls._config.get('actor_run_id')

                if api_url and token and run_id:
                    logger.info('[Scrapely] Initializing API storage for run %s', run_id)
                    smart = _lazy('set_api_storage')({
                        'api_url': api_url,
                        'token': token,
                        'run_id': run_id,
                    })
                elif not cls._config.get('is_at_home'):
                    # Local development without credentials: Smart → FS
                    logger.info('[Scrapely] Initializing local file-system storage')
                    smart = _lazy('set_api_storage')()
                else:
                    raise RuntimeError(
                        'Missing required configuration for secure storage. '
                        'Required: apiBaseUrl (SCRAPELY_API_URL), token (SCRAPELY_TOKEN/APIFY_TOKEN), '
                        'actorRunId (ACTOR_RUN_ID). Containers must be started by the scrapely-server, '
                        'or pass storage in Actor.init().'
                    )

            cls._smart_storage = (
                smart
                if isinstance(smart, _lazy('SmartScrapelyStorageClient'))
                else _lazy('get_smart_storage_client')()
            )
            factory = _lazy('get_api_storage_factory')()
            cls._scrapely_storage = factory
            if factory is not None and factory.has_credentials():
                cls._client = factory.api_client
                await cls._client._ensure_client()

            # Choose event manager AFTER config has effective storage credentials.
            from datetime import timedelta

            ws_url = cls._config.get('actor_events_ws_url')
            persist_interval_ms = cls._config.get('persist_state_interval_millis', 60000)
            persist_interval = timedelta(milliseconds=persist_interval_ms)

            event_manager: EventManager
            if ws_url:
                event_manager = _lazy('ScrapelyEventManager')(
                    configuration=cls._config,
                    persist_state_interval=persist_interval,
                )
            else:
                event_manager = _lazy('LocalEventManager')(
                    system_info_interval=timedelta(seconds=1),
                    persist_state_interval=persist_interval,
                )
            cls._event_manager = event_manager

            await event_manager.__aenter__()

            def handle_signal(signum: int, frame: Any) -> None:
                # Bound drain so uncertain PPE intents can flush before hard exit.
                # Fire-and-forget create_task alone drops pending charges on SIGTERM.
                logger.info('Received signal %s, shutting down...', signum)
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    os._exit(0)  # NoReturn

                if getattr(cls, '_signal_shutdown_started', False):
                    return
                cls._signal_shutdown_started = True

                def _run_shutdown() -> None:
                    async def _bounded() -> None:
                        # Independent hard-exit timer: fires even if teardown hangs on a
                        # stuck close() — the deadline cannot be swallowed by exceptions.
                        hard_timer = loop.call_later(6.0, os._exit, 0)
                        try:
                            await asyncio.wait_for(cls._shutdown(timeout_secs=5), timeout=5.5)
                        except BaseException as err:
                            logger.warning('Shutdown on signal failed: %s', err)
                        finally:
                            hard_timer.cancel()
                            os._exit(0)

                    loop.create_task(_bounded())

                try:
                    loop.call_soon_threadsafe(_run_shutdown)
                except Exception:
                    os._exit(0)

            signal.signal(signal.SIGINT, handle_signal)
            signal.signal(signal.SIGTERM, handle_signal)

            cls._initialized = True

            # Initialize PPE charging once so Actor.charge() has prices + budget.
            # Must run AFTER `_initialized = True`: ChargingManager.init() goes
            # through Actor.get_charging_manager(), which requires an initialized
            # Actor (otherwise this eager init always fails with a warning and
            # charging is only lazily recovered on the first Actor.charge()).
            try:
                charging_manager = cls.get_charging_manager()
                await charging_manager.init()
            except Exception as e:
                logger.warning('Failed to initialize ChargingManager: %s', e)
        except Exception:
            # Roll back partial init so a corrected retry can succeed.
            try:
                await cls._shutdown()
            except Exception as shutdown_err:
                logger.warning('Rollback after failed Actor.init: %s', shutdown_err)
            raise

    @classmethod
    async def _shutdown(cls, timeout_secs: int = 30) -> None:
        # Resource cleanup may raise; config restore + lifecycle reset MUST still
        # run so a failed teardown cannot leave tenant/run credentials active for
        # the next Actor.init() retry.
        cleanup_error: Optional[BaseException] = None
        try:
            if cls._event_manager is not None:
                try:
                    await cls._event_manager.__aexit__(None, None, None)
                except BaseException as e:
                    cleanup_error = cleanup_error or e
                    logger.warning('Event manager shutdown failed: %s', e)
                cls._event_manager = None

            # Best-effort: confirm any unresolved PPE charge intents before teardown.
            if cls._charging_manager is not None:
                # Reject NEW charges before the final flush so nothing can be
                # admitted after it while teardown closes storages/credentials.
                close_fn = getattr(cls._charging_manager, 'close', None)
                if close_fn is not None:
                    try:
                        close_fn()
                    except Exception:
                        pass
                try:
                    flush = getattr(cls._charging_manager, 'flush_pending', None)
                    if flush is not None:
                        deadline_ms = min(max(1, timeout_secs) * 1000, 30_000)
                        # The flush's own deadline ends its retries (charge POSTs back off
                        # for up to ~2 min) a little before wait_for would cancel it.
                        await asyncio.wait_for(
                            flush(max_intents=64, deadline_ms=max(500, deadline_ms - 2_000)),
                            timeout=max(1.0, deadline_ms / 1000.0),
                        )
                except BaseException as e:
                    # Never swallow cancellation — but also never let it skip the
                    # reference-clearing teardown below. Record it and re-raise at
                    # the END of _shutdown: an immediate raise would leave the
                    # closed charging manager and storage/client references
                    # installed, so a subsequent Actor.init() would reuse the
                    # closed manager and every later charge would fail. The
                    # signal-path deadline is still bounded by the independent
                    # loop.call_later(6.0, os._exit, 0) hard timer.
                    cleanup_error = cleanup_error or e
                    logger.warning('ChargingManager flush_pending failed during shutdown: %s', e)

            if cls._scrapely_storage is not None:
                try:
                    await cls._scrapely_storage.close()
                except BaseException as e:
                    cleanup_error = cleanup_error or e
                    logger.warning('Storage close failed during shutdown: %s', e)
                cls._scrapely_storage = None
            cls._smart_storage = None
            cls._client = None
            # Clear charging + patched client so a second Actor lifecycle cannot
            # post charges to the previous run_id / credentials.
            cls._charging_manager = None
            set_active_charging_manager(None)
            cls._apify_client_instance = None

            cls._default_dataset = None
            cls._default_kvs = None
            cls._default_rq = None
            cls._cached_input = None
            try:
                _lazy('disconnect_storage')()
            except BaseException as e:
                cleanup_error = cleanup_error or e
                logger.warning('disconnect_storage failed during shutdown: %s', e)
        finally:
            # ALWAYS restore config keys mutated for this lifecycle so a later
            # init without explicit storage does not inherit previous credentials.
            snap = getattr(cls, '_lifecycle_config_snapshot', None)
            if snap is not None and cls._config is not None:
                for key, value in snap.items():
                    try:
                        cls._config.set(key, value)
                    except Exception:
                        pass
                cls._lifecycle_config_snapshot = None
            cls._initialized = False
            # Allow a subsequent Actor lifecycle to handle SIGINT/SIGTERM again.
            cls._signal_shutdown_started = False

        if cleanup_error is not None:
            raise cleanup_error


    @classmethod
    async def main(cls, user_func: Callable[[], Any]) -> Any:
        try:
            async with cls:
                return await user_func()
        except Exception as e:
            logger.exception(f'Actor failed: {e}')
            cls._exit_code = 1
            raise
        finally:
            sys.exit(cls._exit_code)

    @classmethod
    async def exit(
        cls,
        message_or_options: Optional[Union[str, ExitOptions]] = None,
        options: Optional[ExitOptions] = None,
        *,
        status_message: Optional[str] = None,
        exit_code: Optional[int] = None,
    ) -> None:
        """Exit the Actor with an optional terminal status message.

        Supports keyword-only ``status_message`` and
        ``exit_code`` arguments (``await Actor.exit(status_message="Done")``).
        The pre-existing positional forms (``Actor.exit("message")`` /
        ``Actor.exit(ExitOptions(...))``) remain supported; explicit keyword
        arguments take precedence over the positional values when provided.
        """
        if isinstance(message_or_options, str):
            opts = ExitOptions(status_message=message_or_options)
        else:
            opts = message_or_options or ExitOptions()

        if options:
            opts = ExitOptions(**{**opts.__dict__, **options.__dict__})

        if status_message is not None:
            opts.status_message = status_message
        if exit_code is not None:
            opts.exit_code = exit_code

        if opts.status_message:
            logger.info(f'[Scrapely] {opts.status_message}')

        cls._exit_code = opts.exit_code
        cls._status_message = opts.status_message

        await cls._persist_terminal_status_message(opts.status_message)
        await cls._shutdown(timeout_secs=opts.timeout_secs)

        if opts.exit:
            sys.exit(cls._exit_code)

    @classmethod
    async def fail(
        cls,
        message_or_options: Optional[Union[str, ExitOptions]] = None,
        options: Optional[ExitOptions] = None,
        *,
        status_message: Optional[str] = None,
        exit_code: Optional[int] = None,
        exception: Optional[BaseException] = None,
    ) -> None:
        """Fail the Actor run (exit code defaults to ``1``).

        Supports keyword-only ``status_message``,
        ``exit_code`` and ``exception`` arguments
        (``await Actor.fail(status_message="Boom", exception=e)``). When
        ``exception`` is given its traceback is logged.
        The pre-existing positional forms (``Actor.fail("message")`` /
        ``Actor.fail(ExitOptions(...))``) remain supported; explicit keyword
        arguments take precedence over the positional values when provided.
        """
        if isinstance(message_or_options, str):
            opts = ExitOptions(exit_code=1, status_message=message_or_options)
        else:
            # Options-object form: an explicit exit_code in the object wins over
            # the fail() default of 1 (JS SDK semantics). Build the dict first —
            # passing it alongside exit_code=1 as kwargs raises
            # "got multiple values for keyword argument 'exit_code'".
            base = dict(message_or_options.__dict__) if message_or_options else {}
            base.setdefault('exit_code', 1)
            opts = ExitOptions(**base)

        if options:
            opts = ExitOptions(**{**opts.__dict__, **options.__dict__})

        if status_message is not None:
            opts.status_message = status_message
        if exit_code is not None:
            opts.exit_code = exit_code

        logger.error(
            f'[Scrapely] Actor failed: {opts.status_message or "Unknown error"}',
            exc_info=exception,
        )
        await cls.exit(opts)

    @classmethod
    def is_at_home(cls) -> bool:
        return bool(cls._env_config().get('is_at_home', False))

    @classmethod
    def is_standby(cls) -> bool:
        """Return True when the actor run was started in standby mode (Node SDK ``isStandby`` parity)."""
        return cls._env_config().get('meta_origin') == 'STANDBY'

    @_ClassProperty
    def is_paying(cls) -> bool:
        """Whether the user who started this Actor run is a paying user.

        Reads ``APIFY_USER_IS_PAYING`` (config key
        ``user_is_paying``). Note the platform currently sets this from the
        subscription plan only — PAYG balance alone does not flip it.
        """
        config = cls.get_config() if cls._config is not None else Configuration.get_global_config()
        return bool(config.get('user_is_paying'))

    @classmethod
    def get_env(cls) -> ApifyEnv:
        if cls._config is None:
            cls._config = Configuration.get_global_config()
        return cls._config.get_env()

    @classmethod
    def get_remaining_time(cls) -> Optional[int]:
        timeout_at = os.environ.get('ACTOR_TIMEOUT_AT')
        if not timeout_at:
            return None
        try:
            timeout_date = datetime.fromisoformat(timeout_at.replace('Z', '+00:00'))
            remaining = int((timeout_date - datetime.now(timeout_date.tzinfo)).total_seconds() * 1000)
            return max(0, remaining)
        except (ValueError, TypeError):
            return None

    @classmethod
    async def get_input(cls) -> Optional[Dict[str, Any]]:
        cls._ensure_initialized('get_input')

        if cls._cached_input is not None:
            return cls._cached_input

        metamorph_count = os.environ.get('ACTOR_METAMORPH_COUNT')
        if metamorph_count:
            count = int(metamorph_count)
            if count > 0:
                kvs = await cls.open_key_value_store()
                metamorph_input = await kvs.get_value(f'INPUT-METAMORPH-{count}')
                if metamorph_input is not None:
                    cls._cached_input = with_proxy_key_aliases(metamorph_input)
                    return cast(Dict[str, Any], cls._cached_input)

        raw_input = os.environ.get('ACTOR_INPUT')
        if raw_input:
            try:
                decoded = base64.b64decode(raw_input).decode('utf-8')
                parsed = json.loads(decoded)
            except Exception:
                parsed = json.loads(raw_input)
            # Decrypt secret inputs after loading (JS parity: only the main input
            # path is decrypted, mirroring Actor.getInput() in the Node SDK)
            if isinstance(parsed, dict):
                parsed = decrypt_secret_inputs(parsed)
            # Proxy objects carry both namings (scrapelyProxy* and apifyProxy*).
            cls._cached_input = with_proxy_key_aliases(parsed)
            return cast(Dict[str, Any], cls._cached_input)

        kvs = await cls.open_key_value_store()
        input_key = cls._env_config().get('input_key', 'INPUT')
        result = await kvs.get_value(input_key)
        if isinstance(result, dict):
            result = decrypt_secret_inputs(result)
        cls._cached_input = with_proxy_key_aliases(result)
        return cast(Optional[Dict[str, Any]], cls._cached_input)

    @classmethod
    async def get_input_or_throw(cls) -> Dict[str, Any]:
        result = await cls.get_input()
        if result is None:
            raise ValueError('Input does not exist')
        return result

    @_ClassProperty
    def input(cls) -> Optional[Dict[str, Any]]:
        if cls._cached_input is not None:
            return cls._cached_input

        raw = os.environ.get('ACTOR_INPUT')
        if not raw:
            return None

        try:
            decoded = base64.b64decode(raw).decode('utf-8')
            cls._cached_input = json.loads(decoded)
        except Exception:
            try:
                cls._cached_input = json.loads(raw)
            except json.JSONDecodeError:
                return None

        # Decrypt secret inputs after loading (JS parity with Actor.input getter)
        if isinstance(cls._cached_input, dict):
            cls._cached_input = with_proxy_key_aliases(decrypt_secret_inputs(cls._cached_input))

        return cls._cached_input

    @classmethod
    async def push_data(
        cls,
        data: Union[Dict[str, Any], List[Dict[str, Any]]],
        charged_event_name: Optional[str] = None,
        *,
        event_name: Optional[str] = None,
        charges: Optional[Dict[str, int]] = None,
        charge_per: str = 'row',
        dataset: Optional[Union[str, Dataset]] = None,
    ) -> ChargeResult:
        """Push data to the default dataset, charging pay-per-event items.

        Usage: ``Actor.push_data(data, charged_event_name=...)``.
        When the Actor uses pay-per-event pricing, each pushed item is charged for the
        default-dataset-item event (if priced) and, when ``charged_event_name`` is
        provided, for that explicit event as well. Actors without pay-per-event
        pricing push without charging and receive an empty result.

        Args:
            data: Item or list of items to push to the default dataset.
            charged_event_name: Optional pricing event to charge for each pushed item
                (also accepted positionally).
            event_name: Deprecated alias of ``charged_event_name`` (JS ``eventName``),
                kept so existing callers keep working.
            charges: Several events per row, charged atomically: ``{'result': 1, 'page': 3}``
                charges 1 ``result`` and 3 ``page`` events for every row. A row is stored
                only when all of its events were charged; rows the spending limit cannot
                pay for in full are not stored. Cannot be combined with ``charged_event_name``.
            charge_per: ``'row'`` (default) charges ``charges`` for every row; ``'batch'``
                charges them once for the whole push, e.g.
                ``push_data(page_rows, charges={'document': 1}, charge_per='batch')``:
                all rows are stored if the charge is accepted, none if it is refused.
            dataset: Push to this named dataset (name, or a Dataset from ``open_dataset``)
                instead of the default one, with the same charge-first semantics:
                ``push_data(rows, charged_event_name='comment', dataset=f'comments-{run_id}')``.
                Rows of a named dataset carry no implicit default-dataset-item event and are
                not gated by the default dataset's spending-limit safety net; charged pushes
                store only the rows that were charged.

        Returns:
            ChargeResult describing how many items were charged and pushed.

        Raises:
            ValueError: If the event refers to a synthetic event, or both names are given
                with different values.
        """
        cls._ensure_initialized('push_data')
        if charged_event_name is not None and event_name is not None and charged_event_name != event_name:
            raise ValueError('Pass only one of charged_event_name / event_name')
        event_name = charged_event_name if charged_event_name is not None else event_name
        # Exact synthetic names only (JS parity) — a prefix heuristic would block
        # real priced events.
        if event_name and is_default_dataset_item_event(event_name):
            raise ValueError(f"Cannot charge for synthetic event '{event_name}' manually")

        if cls._default_dataset is None:
            cls._default_dataset = await cls.open_dataset()

        target = cls._default_dataset
        if dataset is not None:
            target = dataset if isinstance(dataset, _lazy('Dataset')) else await cls.open_dataset(name=dataset)
        is_default = target is cls._default_dataset or (
            getattr(target, 'id', None) is not None and target.id == getattr(cls._default_dataset, 'id', None)
        )

        items = data if isinstance(data, list) else [data]
        charging_manager = cls.get_charging_manager()
        if charges is not None:
            if event_name is not None:
                raise ValueError('Pass either charged_event_name or charges, not both')
            return await push_data_with_charges(
                charging_manager,
                items,
                charges,
                is_default,
                target.push_data,
                charge_per=charge_per,
            )
        if charge_per != 'row':
            raise ValueError("charge_per needs charges={...}")
        return await push_data_and_charge(
            charging_manager,
            items,
            event_name,
            is_default,
            target.push_data,
        )

    @classmethod
    async def get_value(cls, key: str, default_value: Any = None) -> Any:
        cls._ensure_initialized('get_value')
        if cls._default_kvs is None:
            cls._default_kvs = await cls.open_key_value_store()
        return await cls._default_kvs.get_value(key, default_value=default_value)

    @classmethod
    async def set_value(cls, key: str, value: Any, content_type: Optional[str] = None) -> None:
        cls._ensure_initialized('set_value')
        if cls._default_kvs is None:
            cls._default_kvs = await cls.open_key_value_store()
        await cls._default_kvs.set_value(key, value, content_type=content_type)

    # Debug capture: raw source payloads for platform diagnostics. Only active when the platform
    # sets SCRAPELY_DEBUG_CAPTURE=1 on its own diagnostic runs; a no-op in every other run.
    # A payload up to _DEBUG_CAPTURE_MAX_BYTES characters is stored as it is; a larger one gzip-compressed
    # (base64, at most _DEBUG_CAPTURE_MAX_BYTES characters again), keeping up to _DEBUG_CAPTURE_MAX_CHARS
    # characters / _DEBUG_CAPTURE_MAX_ENCODED bytes of it. A plain cut at 256 KiB lost the part a check
    # needed when a large API document starts with site navigation (the seller section of a 581 KB
    # listing JSON sat at ~333 KB).
    _DEBUG_CAPTURE_MAX_BYTES = 256 * 1024
    _DEBUG_CAPTURE_MAX_CHARS = 8 * 1024 * 1024
    _DEBUG_CAPTURE_MAX_ENCODED = 8 * 1024 * 1024
    _DEBUG_CAPTURE_MAX_RECORDS = 500
    _debug_capture_count = 0

    @staticmethod
    def debug_capture_record_key(key: str) -> str:
        """The key-value store record name for a capture key (item URL or id)."""
        import hashlib
        return 'DEBUG_CAPTURE_' + hashlib.sha1(str(key).strip().encode('utf-8')).hexdigest()[:24]

    _main_loop: Optional[asyncio.AbstractEventLoop] = None

    @classmethod
    def debug_capture_sync(cls, key: str, payload: Any) -> None:
        """debug_capture for synchronous code (also from worker threads, e.g. asyncio.to_thread).
        Does nothing unless the platform enabled capture for this run."""
        if os.environ.get('SCRAPELY_DEBUG_CAPTURE') != '1' or not key:
            return
        loop = cls._main_loop
        if loop is None or loop.is_closed():
            return
        try:
            try:
                running = asyncio.get_running_loop()
            except RuntimeError:
                running = None
            if running is loop:
                loop.create_task(cls.debug_capture(key, payload))
            else:
                asyncio.run_coroutine_threadsafe(cls.debug_capture(key, payload), loop).result(timeout=15)
        except Exception as err:  # never break the run
            logging.getLogger('scrapely').debug('debug_capture_sync failed: %s', err)

    @classmethod
    async def debug_capture(cls, key: str, payload: Any) -> None:
        """Save the raw source payload (API JSON or HTML) an item was extracted from, keyed by
        the item's URL or id. Does nothing unless the platform enabled capture for this run."""
        if os.environ.get('SCRAPELY_DEBUG_CAPTURE') != '1' or not key:
            return
        if cls._debug_capture_count >= cls._DEBUG_CAPTURE_MAX_RECORDS:
            return
        try:
            markup = _element_markup(payload)
            if markup is not None:
                text = markup
            elif isinstance(payload, (bytes, bytearray)):
                text = bytes(payload).decode('utf-8', 'replace')
            elif isinstance(payload, str):
                text = payload
            else:
                text = json.dumps(payload, default=_capture_json_default, ensure_ascii=False, separators=(',', ':'))
            record = cls._debug_capture_record(str(key), text)
            cls._debug_capture_count += 1
            await cls.set_value(cls.debug_capture_record_key(key), record)
        except Exception as err:  # never break the run
            logging.getLogger('scrapely').debug('debug_capture failed: %s', err)

    @classmethod
    def _debug_capture_record(cls, key: str, text: str) -> Dict[str, Any]:
        """The record for one capture: plain when small, else gzip-base64 (plain head as the last resort)."""
        limit = cls._DEBUG_CAPTURE_MAX_BYTES
        if len(text) <= limit:
            return {'key': key, 'payload': text}
        import gzip
        raw = text[: cls._DEBUG_CAPTURE_MAX_CHARS].encode('utf-8')
        kept = raw[: cls._DEBUG_CAPTURE_MAX_ENCODED]
        packed = base64.b64encode(gzip.compress(kept, 6)).decode('ascii')
        if len(packed) <= limit:
            truncated = len(text) > cls._DEBUG_CAPTURE_MAX_CHARS or len(kept) < len(raw)
            return {'key': key, 'payload': packed, 'encoding': 'gzip-base64', 'length': len(text), 'truncated': truncated}
        return {'key': key, 'payload': text[:limit], 'length': len(text), 'truncated': True}

    @classmethod
    def _resolve_open_args(
        cls,
        name_or_id: Optional[str] = None,
        *,
        id: Optional[str] = None,
        name: Optional[str] = None,
        alias: Optional[str] = None,
        force_cloud: bool = False,
        options: Optional[OpenStorageOptions] = None,
        storage_type: StorageType = 'datasets',
    ) -> Dict[str, Any]:
        force = force_cloud or bool(options and options.force_cloud)

        if name_or_id is not None:
            if id is not None or name is not None or alias is not None:
                raise ValueError('Pass either positional name_or_id or id/name/alias kwargs, not both')
            if is_storage_uuid(name_or_id):
                id = name_or_id
            else:
                name = name_or_id

        if alias is not None:
            storages_json = cls._env_config().get('actor_storages_json')
            resolved = _lazy('resolve_alias_from_storages_json')(alias, storage_type, storages_json)
            if resolved is not None:
                id = resolved
                alias = None
            else:
                # Outside platform: alias behaves like a name (TS)
                name = alias
                alias = None

        return {'id': id, 'name': name, 'alias': alias, 'force_cloud': force}

    @classmethod
    def _storage_client_for_open(cls, *, force_cloud: bool = False) -> Optional[StorageClient]:
        smart = cls._smart_storage or _lazy('get_smart_storage_client')()
        if smart is not None:
            return smart.get_suitable_storage_client(force_cloud=force_cloud)

        if force_cloud:
            cloud = _lazy('get_api_storage_factory')()
            if cloud is None:
                # Build from config like TS _buildStorageOpenOptions
                config = cls.get_config()
                api_url = config.get('api_base_url')
                token = config.get('token')
                run_id = config.get('actor_run_id')
                if api_url and token and run_id:
                    return _lazy('ScrapelyStorageClient')(api_url=api_url, token=token, run_id=run_id)
                raise RuntimeError('Cloud storage client is not configured')
            return cloud
        return None

    @classmethod
    async def open_dataset(
        cls,
        name_or_id: Optional[str] = None,
        options: Optional[OpenStorageOptions] = None,
        *,
        id: Optional[str] = None,
        name: Optional[str] = None,
        alias: Optional[str] = None,
        force_cloud: bool = False,
    ) -> Dataset:
        cls._ensure_initialized('open_dataset')
        open_args = cls._resolve_open_args(
            name_or_id,
            id=id,
            name=name,
            alias=alias,
            force_cloud=force_cloud,
            options=options,
            storage_type='datasets',
        )
        storage_client = cls._storage_client_for_open(force_cloud=open_args['force_cloud'])

        if open_args['id'] is None and open_args['name'] is None and open_args['alias'] is None:
            if cls._default_dataset is None:
                cls._default_dataset = await _lazy('Dataset').open(storage_client=storage_client)
            return cls._default_dataset

        return await _lazy('Dataset').open(
            id=open_args['id'],
            name=open_args['name'],
            alias=open_args['alias'],
            storage_client=storage_client,
        )

    @classmethod
    async def open_key_value_store(
        cls,
        name_or_id: Optional[str] = None,
        options: Optional[OpenStorageOptions] = None,
        *,
        id: Optional[str] = None,
        name: Optional[str] = None,
        alias: Optional[str] = None,
        force_cloud: bool = False,
    ) -> KeyValueStore:
        cls._ensure_initialized('open_key_value_store')
        open_args = cls._resolve_open_args(
            name_or_id,
            id=id,
            name=name,
            alias=alias,
            force_cloud=force_cloud,
            options=options,
            storage_type='keyValueStores',
        )
        storage_client = cls._storage_client_for_open(force_cloud=open_args['force_cloud'])

        if open_args['id'] is None and open_args['name'] is None and open_args['alias'] is None:
            if cls._default_kvs is None:
                cls._default_kvs = await _lazy('KeyValueStore').open(storage_client=storage_client)
            return cls._default_kvs

        return await _lazy('KeyValueStore').open(
            id=open_args['id'],
            name=open_args['name'],
            alias=open_args['alias'],
            storage_client=storage_client,
        )

    @classmethod
    async def open_request_queue(
        cls,
        name_or_id: Optional[str] = None,
        options: Optional[OpenStorageOptions] = None,
        *,
        id: Optional[str] = None,
        name: Optional[str] = None,
        alias: Optional[str] = None,
        force_cloud: bool = False,
    ) -> RequestQueue:
        cls._ensure_initialized('open_request_queue')
        open_args = cls._resolve_open_args(
            name_or_id,
            id=id,
            name=name,
            alias=alias,
            force_cloud=force_cloud,
            options=options,
            storage_type='requestQueues',
        )
        storage_client = cls._storage_client_for_open(force_cloud=open_args['force_cloud'])

        if open_args['id'] is None and open_args['name'] is None and open_args['alias'] is None:
            if cls._default_rq is None:
                cls._default_rq = await _lazy('RequestQueue').open(storage_client=storage_client)
            return cls._default_rq

        return await _lazy('RequestQueue').open(
            id=open_args['id'],
            name=open_args['name'],
            alias=open_args['alias'],
            storage_client=storage_client,
        )

    @classmethod
    def on(cls, event: Event, listener: Callable) -> None:
        cls._ensure_initialized('on')
        if cls._event_manager is None:
            raise RuntimeError('Actor event manager is not initialized.')
        cls._event_manager.on(event=event, listener=listener)

    @classmethod
    def off(cls, event: Event, listener: Optional[Callable] = None) -> None:
        cls._ensure_initialized('off')
        if cls._event_manager is None:
            raise RuntimeError('Actor event manager is not initialized.')
        cls._event_manager.off(event=event, listener=listener)

    @classmethod
    async def set_status_message(
        cls,
        message: str,
        options: Optional[SetStatusMessageOptions] = None,
        *,
        is_terminal: Optional[bool] = None,
    ) -> ActorRun:
        """Set the status message for the current Actor run (Node SDK parity).

        Logs the message at the requested level, then updates the run through
        the platform API when credentials (``api_base_url``, ``token``,
        ``actor_run_id``) are available. Locally, logs only and returns an
        empty run object, mirroring the Node SDK behavior.

        Args:
            message: The status message to set.
            options: Optional settings (``is_status_message_terminal``, ``level``).
            is_terminal: Terminal flag. When given, it
                overrides ``options.is_status_message_terminal``.

        Returns:
            The updated ActorRun object, or an empty ActorRun when running locally.
        """
        cls._ensure_initialized('set_status_message')
        opts = options or SetStatusMessageOptions()
        level = (getattr(opts, 'level', None) or 'INFO').upper()
        if is_terminal is None:
            is_terminal = bool(getattr(opts, 'is_status_message_terminal', False))

        logged = f'[Status message]: {message}'
        if level == 'DEBUG':
            logger.debug(logged)
        elif level == 'WARNING':
            logger.warning(logged)
        elif level == 'ERROR':
            logger.error(logged)
        else:
            logger.info(logged)

        if is_terminal:
            result = await cls._put_run_status_message(message, is_terminal=True)
        else:
            # A progress update is best effort: if the API still fails after the
            # retry window (e.g. a long 503), log it and carry on instead of
            # crashing the run. Terminal updates keep raising.
            try:
                result = await cls._put_run_status_message(message, is_terminal=False)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                logger.warning('Failed to update the status message (continuing): %s', e)
                return ActorRun(id='', actor_id='', status='')
        if result is None:
            return ActorRun(id='', actor_id='', status='')
        run_id, data = result

        return ActorRun(
            id=data.get('id', run_id),
            actor_id=data.get('actorId', ''),
            status=data.get('status', ''),
            started_at=data.get('startedAt'),
            finished_at=data.get('finishedAt'),
            status_message=data.get('statusMessage'),
        )

    @classmethod
    async def solver_status(cls, vendor: str) -> Dict[str, Any]:
        """Anti-bot solver status for a vendor (free, cached for 15 s, never raises).

        ``usable`` False means the vendor cannot be solved right now: take your
        fallback (for example the hosted browser), or fail the items that need
        a solve at once and call ``set_solver_degraded_status()``. See
        ``scrapely.solver``.
        """
        from .solver import solver_status_async

        return await solver_status_async(vendor)

    @classmethod
    async def set_solver_degraded_status(cls, skipped: Optional[int] = None, total: Optional[int] = None) -> None:
        """Set the standard status message for a run whose site protection cannot be solved right now.

        "Site protection temporarily unavailable, results may be incomplete",
        with "(N of M items skipped)" when both counts are given. Never raises:
        the run goes on.
        """
        from .solver import degraded_status_message

        try:
            await cls.set_status_message(degraded_status_message(skipped, total))
        except Exception as exc:  # noqa: BLE001 - a status message must never stop the run
            logger.warning(f'Could not set the solver status message: {exc}')

    @classmethod
    async def _put_run_status_message(
        cls, message: str, *, is_terminal: bool
    ) -> Optional[Tuple[str, Dict[str, Any]]]:
        """PUT the status message to the current run. Returns ``None`` when not on the platform."""
        config = cls.get_config()
        api_url = config.get('api_base_url')
        token = config.get('token')
        run_id = config.get('actor_run_id')

        if not api_url or not token or not run_id:
            return None

        client = cls.new_client(token=token, base_url=api_url)
        # Setting the message is idempotent: a 429 (run API rate limit), 5xx or
        # dropped connection is retried with backoff instead of failing the run.
        response = await client.put(
            f'/v2/actor-runs/{run_id}',
            json_data={
                'statusMessage': message,
                'isStatusMessageTerminal': is_terminal,
            },
            idempotent=True,
        )
        body: Any = {}
        if response.content:
            body = response.json()
        data = body.get('data', body) if isinstance(body, dict) else {}
        return run_id, data if isinstance(data, dict) else {}

    @classmethod
    async def _persist_terminal_status_message(cls, message: Optional[str], timeout_secs: float = 10.0) -> None:
        """Store the exit/fail status message on the run as terminal.

        Runs before teardown so it lands while the run still exists and the
        process is alive, on success and failure alike. Best effort: a failed
        update is logged and never blocks the exit.
        """
        if not message or not cls._initialized or cls._config is None:
            return
        try:
            await asyncio.wait_for(
                cls._put_run_status_message(message, is_terminal=True),
                timeout=max(1.0, timeout_secs),
            )
        except asyncio.CancelledError:
            raise
        except BaseException as e:
            logger.warning('Failed to store the final status message on the run: %s', e)

    @_ClassProperty
    def log(cls) -> logging.Logger:
        return logger

    @_ClassProperty
    def exit_code(cls) -> int:
        return cls._exit_code

    @_ClassProperty
    def status_message(cls) -> Optional[str]:
        return cls._status_message

    @classmethod
    def _ensure_initialized(cls, method_name: str) -> None:
        if not cls._initialized:
            raise RuntimeError(
                f'Actor.{method_name}() requires Actor.init() to be called first. '
                'Hint: Use async with Actor: which calls init() for you.'
            )

    @classmethod
    def get_config(cls) -> Configuration:
        if cls._config is None:
            cls._config = Configuration.get_global_config()
        return cls._config

    @classmethod
    def _env_config(cls) -> Configuration:
        # is_at_home() and friends read the environment even
        # before init(). Doesn't set cls._config, so init() state is unchanged.
        return cls._config if cls._config is not None else Configuration.get_global_config()

    @classmethod
    def new_client(cls, token: Optional[str] = None, base_url: Optional[str] = None) -> ApiClient:
        config = cls.get_config()
        api_url = base_url or config.get('api_base_url')
        api_token = token or config.get('token')
        run_id = config.get('actor_run_id', 'unknown')
        return ApiClient(api_url=api_url, token=api_token, run_id=run_id)

    # -------------------------------------------------------------------------
    # Client and configuration properties
    # -------------------------------------------------------------------------

    @classmethod
    def _get_apify_client(cls) -> 'ApifyClientAsync':
        """Get or create a cached charging-configured ``ApifyClientAsync`` instance.
        
        This client provides the full ApifyClient API including
        actor(), dataset(), key_value_store(), etc. methods.
        """
        if cls._apify_client_instance is None:
            config = cls.get_config()
            api_url = config.get('api_base_url')
            token = config.get('token')
            default_dataset_id = config.get('default_dataset_id')

            # Get charging manager if available
            charging_manager = None
            try:
                charging_manager = cls.get_charging_manager()
            except RuntimeError:
                pass  # Actor not initialized yet

            cls._apify_client_instance = create_charging_client(
                token=token,
                api_url=api_url,
                # Public URLs (record/keys public URLs) must be minted against the
                # configured public host, not the client's built-in default.
                api_public_url=config.get('api_public_base_url'),
                charging_manager=charging_manager,
                default_dataset_id=default_dataset_id,
            )
        
        return cls._apify_client_instance

    @_ClassProperty
    def apify_client(cls) -> 'ApifyClientAsync':
        """Get a charging-configured ``ApifyClientAsync``.
        
        This provides the full ApifyClient API including:
        - client.actor('id').call()
        - client.dataset('id').list_items()
        - client.key_value_store('id').get_record('key')
        - client.run('id').wait()
        - etc.
        """
        return cls._get_apify_client()

    @_ClassProperty
    def configuration(cls) -> Configuration:
        """Return the Configuration instance."""
        return cls.get_config()

    @_ClassProperty
    def config(cls) -> Configuration:
        """Return the Configuration instance."""
        return cls.get_config()

    @_ClassProperty
    def event_manager(cls) -> Optional[EventManager]:
        """Return the EventManager instance."""
        return cls._event_manager

    # -------------------------------------------------------------------------
    # Charging methods
    # -------------------------------------------------------------------------

    @classmethod
    def get_charging_manager(cls) -> ChargingManager:
        """Get the ChargingManager instance for pay-per-event pricing.
        
        Returns:
            ChargingManager instance
            
        Raises:
            RuntimeError: If Actor is not initialized
        """
        cls._ensure_initialized('get_charging_manager')
        config = cls.get_config()
        effective_run_id = config.get('actor_run_id')

        # Recreate when missing or bound to a stale run (e.g. sequential lifecycles
        # that forgot to clear, or storage credentials changed after init).
        if cls._charging_manager is not None:
            manager_run_id = getattr(cls._charging_manager, '_actor_run_id', None)
            if manager_run_id != effective_run_id:
                cls._charging_manager = None
                cls._apify_client_instance = None

        if cls._charging_manager is None:
            # Prefer the storage factory client (same URL/token/run as API storage).
            client = cls._client if cls._client is not None else cls.new_client()
            cls._charging_manager = ChargingManager(config, client)
            # Storage clients refuse default-dataset writes past this run's limit.
            set_active_charging_manager(cls._charging_manager)
        return cls._charging_manager


    @classmethod
    async def charge(cls, event_name: str, count: int = 1) -> ChargeResult:
        """Charge for a specified number of events - sub-operations of the Actor.

        This is relevant only for the pay-per-event pricing model.

        Args:
            event_name: Name of the event to be charged for
            count: Number of events to charge for

        Returns:
            ChargeResult indicating if the limit was reached
        """
        cls._ensure_initialized('charge')
        charging_manager = cls.get_charging_manager()
        if not getattr(charging_manager, '_initialized', False):
            await charging_manager.init()
        return await charging_manager.charge(event_name, count=count)

    # -------------------------------------------------------------------------
    # Actor run control methods
    # -------------------------------------------------------------------------

    @classmethod
    async def abort(
        cls,
        run_id: Optional[str] = None,
        options: Optional[AbortOptions] = None,
    ) -> ActorRun:
        """Abort an Actor run on the Scrapely platform.

        Args:
            run_id: The ID of the run to abort. Defaults to the current run.
            options: Abort options including token, status_message, gracefully

        Returns:
            ActorRun object with details about the aborted run
        """
        cls._ensure_initialized('abort')
        config = cls.get_config()
        target_run_id = run_id or config.get('actor_run_id')
        if not target_run_id:
            raise ValueError('Actor.abort() requires a run ID')

        opts = options or AbortOptions()
        client = cls.new_client(token=opts.token) if opts.token else cls.new_client()

        # Set status message if provided
        if opts.status_message:
            await client.put_json(
                f'/v2/actor-runs/{target_run_id}',
                data={'statusMessage': opts.status_message},
            )

        # Abort the run; `gracefully` travels as a query parameter (the server
        # reads it only from the query string) and is omitted when unset so the
        # server default applies (Node SDK parity).
        abort_params: Dict[str, Any] = {}
        if opts.gracefully is not None:
            abort_params['gracefully'] = 'true' if opts.gracefully else 'false'
        data = _payload_dict(
            await client.post_json(
                f'/v2/actor-runs/{target_run_id}/abort', data=None, params=abort_params or None
            )
        )
        
        return ActorRun(
            id=data.get('id', target_run_id),
            actor_id=data.get('actorId', ''),
            status=data.get('status', 'ABORTED'),
            started_at=data.get('startedAt'),
            finished_at=data.get('finishedAt'),
            status_message=data.get('statusMessage'),
        )

    @classmethod
    async def start(
        cls,
        actor_id: str,
        run_input: Optional[Any] = None,
        options: Optional[StartOptions] = None,
    ) -> ActorRun:
        """Start an Actor on the Scrapely platform without waiting for finish.

        Args:
            actor_id: The ID of the Actor to start
            run_input: The input to pass to the Actor run
            options: Start options including token, build, memory_mbytes, etc.

        Returns:
            ActorRun object with details about the started run
        """
        cls._ensure_initialized('start')
        opts = options or StartOptions()
        client = cls.new_client(token=opts.token) if opts.token else cls.new_client()

        # Wire format (server contract): the POST body is the run input itself;
        # run options are query parameters. Inline webhooks ride
        # along base64-encoded in the `webhooks` query param.
        params = _run_options_params(
            build=opts.build,
            timeout=opts.timeout,
            memory_mbytes=opts.memory_mbytes,
            max_total_charge_usd=opts.max_total_charge_usd,
            restart_on_error=opts.restart_on_error,
            force_permission_level=opts.force_permission_level,
            webhooks=opts.webhooks,
        )

        data = _payload_dict(
            await client.post_json(f'/v2/actors/{actor_id}/runs', data=run_input, params=params or None)
        )

        return ActorRun(
            id=data.get('id', ''),
            actor_id=data.get('actorId', actor_id),
            status=data.get('status', 'RUNNING'),
            started_at=data.get('startedAt'),
            build_id=data.get('buildId'),
            default_dataset_id=data.get('defaultDatasetId'),
            default_key_value_store_id=data.get('defaultKeyValueStoreId'),
            default_request_queue_id=data.get('defaultRequestQueueId'),
        )

    @classmethod
    async def call(
        cls,
        actor_id: str,
        run_input: Optional[Any] = None,
        options: Optional[CallOptions] = None,
    ) -> ActorRun:
        """Start an Actor and wait for it to finish.

        Args:
            actor_id: The ID of the Actor to call
            run_input: The input to pass to the Actor run
            options: Call options including token, build, timeout, etc.

        Returns:
            ActorRun object with details about the finished run
        """
        cls._ensure_initialized('call')
        opts = options or CallOptions()
        
        # Resolve the timeout once: 'inherit' becomes the remaining seconds of
        # this run, forwarded to the server AND used as the poll deadline.
        timeout_secs = _resolve_run_timeout(opts.timeout)

        # Start the actor (timeout/webhooks forwarded so the server enforces
        # the run timeout and registers inline webhooks).
        start_opts = StartOptions(
            token=opts.token,
            build=opts.build,
            memory_mbytes=opts.memory_mbytes,
            max_total_charge_usd=opts.max_total_charge_usd,
            restart_on_error=opts.restart_on_error,
            timeout=timeout_secs,
            force_permission_level=opts.force_permission_level,
            webhooks=opts.webhooks,
        )
        run = await cls.start(actor_id, run_input, start_opts)

        # Wait for finish (honors opts.timeout; polls indefinitely when unset)
        client = cls.new_client(token=opts.token) if opts.token else cls.new_client()
        return await cls._wait_for_terminal_run(
            client,
            run.id,
            actor_id_fallback=actor_id,
            timeout_seconds=timeout_secs,
        )

    @classmethod
    async def _wait_for_terminal_run(
        cls,
        client: ApiClient,
        run_id: str,
        actor_id_fallback: str = '',
        timeout_seconds: Optional[int] = None,
    ) -> ActorRun:
        """Poll ``GET /v2/actor-runs/:runId`` until the run reaches a terminal status.

        When ``timeout_seconds`` elapses first, the run is aborted (best-effort)
        and ``TimeoutError`` is raised.
        """
        deadline = time.monotonic() + timeout_seconds if timeout_seconds is not None else None

        while True:
            try:
                data = _payload_dict(await client.get_json(f'/v2/actor-runs/{run_id}'))
            except (ApiClientError, ForbiddenError) as exc:
                if getattr(exc, 'status_code', None) != 403:
                    raise
                # The token cannot observe this run (e.g. a LIMITED_PERMISSIONS
                # run token polling a run it did not start). Fail fast with an
                # actionable message instead of a bare 403 so callers know the
                # child run may still be running unobserved.
                # NOTE: the real ApiClient raises ApiClientError for every
                # non-2xx response; ForbiddenError is kept for parity with
                # callers that surface the typed errors from scrapely.errors.
                raise RuntimeError(
                    f'Actor: cannot observe run {run_id} with the current token (403 Forbidden). '
                    'A LIMITED_PERMISSIONS run token may only poll or abort runs it started itself; '
                    'the run may still be executing. Started from another actor? Pass a user API '
                    'token via options.token.'
                ) from exc
            status = data.get('status', '').upper()

            if status in _RUN_TERMINAL_STATUSES:
                return ActorRun(
                    id=data.get('id', run_id),
                    actor_id=data.get('actorId', actor_id_fallback),
                    status=status,
                    started_at=data.get('startedAt'),
                    finished_at=data.get('finishedAt'),
                    duration_millis=data.get('durationMillis'),
                    exit_code=data.get('exitCode'),
                    status_message=data.get('statusMessage'),
                    default_dataset_id=data.get('defaultDatasetId'),
                    default_key_value_store_id=data.get('defaultKeyValueStoreId'),
                    default_request_queue_id=data.get('defaultRequestQueueId'),
                )

            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    try:
                        await client.post_json(f'/v2/actor-runs/{run_id}/abort', data=None)
                    except Exception:
                        logger.warning('Actor: best-effort abort of run %s after timeout failed', run_id)
                    raise TimeoutError(
                        f'The actor run {run_id} did not finish within {timeout_seconds} seconds. '
                        'The run was aborted.'
                    ) from None
                await asyncio.sleep(min(1.0, remaining))
            else:
                await asyncio.sleep(1)

    @classmethod
    async def call_task(
        cls,
        task_id: str,
        task_input: Optional[Dict[str, Any]] = None,
        options: Optional[CallOptions] = None,
    ) -> ActorRun:
        """Start an Actor task and wait for it to finish.

        Args:
            task_id: The ID of the task to call
            task_input: Overrides the input for the task
            options: Call options including token, build, timeout, etc.

        Returns:
            ActorRun object with details about the finished run
        """
        cls._ensure_initialized('call_task')
        opts = options or CallOptions()
        client = cls.new_client(token=opts.token) if opts.token else cls.new_client()

        # Wire format (server contract): the POST body is the task input
        # override (merged over the task's saved input by the server); run
        # options are query parameters. The task-run endpoint is
        # /v2/actor-tasks/:taskId/runs (there is no /v2/tasks/:taskId/runs).
        params = _run_options_params(
            build=opts.build,
            timeout=opts.timeout,
            memory_mbytes=opts.memory_mbytes,
            max_total_charge_usd=opts.max_total_charge_usd,
            restart_on_error=opts.restart_on_error,
            force_permission_level=opts.force_permission_level,
            webhooks=opts.webhooks,
        )

        data = _payload_dict(
            await client.post_json(f'/v2/actor-tasks/{task_id}/runs', data=task_input, params=params or None)
        )
        run_id = data.get('id', '')
        if not run_id:
            raise RuntimeError(f'Actor.call_task(): creating a run for task {task_id} returned no run ID')

        # Wait for finish (honors opts.timeout, incl. 'inherit'; polls
        # indefinitely when unset)
        return await cls._wait_for_terminal_run(
            client,
            run_id,
            timeout_seconds=_resolve_run_timeout(opts.timeout),
        )

    # -------------------------------------------------------------------------
    # Platform integration methods
    # -------------------------------------------------------------------------

    @classmethod
    async def metamorph(
        cls,
        target_actor_id: str,
        run_input: Optional[Any] = None,
        options: Optional[MetamorphOptions] = None,
    ) -> None:
        """Transform this Actor run to an Actor run of a different Actor.

        The platform stops the current Actor container and starts a new container
        with the new Actor instead. All default storages are preserved, and the
        new input is stored under the INPUT-METAMORPH-1 key in the same default
        key-value store.

        Args:
            target_actor_id: ID of the target Actor to transform into
            run_input: The input to pass to the new run
            options: Metamorph options including build, content_type
        """
        cls._ensure_initialized('metamorph')
        
        if not cls.is_at_home():
            logger.warning('Actor.metamorph() is only supported when running on the Scrapely platform.')
            return

        config = cls.get_config()
        run_id = config.get('actor_run_id')
        if not run_id:
            raise RuntimeError('actor_run_id is not set - metamorph requires running on the platform')

        opts = options or MetamorphOptions()

        # Wire format (server contract): the new input (if any) goes in the
        # request body's `input` field; the server stores it as
        # INPUT-METAMORPH-<n> (INPUT keeps the original) and tracks the
        # metamorph count in run stats. targetActorId/build are accepted in the body.
        client = cls.new_client()
        body: Dict[str, Any] = {'targetActorId': target_actor_id}
        if opts.build:
            body['build'] = opts.build
        if run_input is not None:
            body['input'] = run_input

        logger.info(f'[Scrapely] Metamorphosing to actor {target_actor_id}')

        await client.post_json(f'/v2/actor-runs/{run_id}/metamorph', data=body)

        # Flush pending PPE intents before hard exit.
        try:
            await cls._shutdown(timeout_secs=5)
        except Exception as err:
            logger.warning('flush_pending during metamorph failed: %s', err)

        # Exit - the platform will restart with new actor
        logger.info('[Scrapely] Metamorph request accepted, exiting...')
        sys.exit(0)

    @classmethod
    async def reboot(cls, options: Optional[RebootOptions] = None) -> None:
        """Internally reboot this Actor.

        The system stops the current container and starts a new one with the
        same run ID and default storages.

        Args:
            options: Reboot options including custom_after_sleep_millis
        """
        cls._ensure_initialized('reboot')
        
        if not cls.is_at_home():
            logger.warning('Actor.reboot() is only supported when running on the Scrapely platform.')
            return

        config = cls.get_config()
        run_id = config.get('actor_run_id')
        if not run_id:
            raise RuntimeError('actor_run_id is not set - reboot requires running on the platform')

        opts = options or RebootOptions()

        # Emit persist state event
        if cls._event_manager:
            from crawlee.events import EventPersistStateData
            cls._event_manager.emit(
                event=_lazy('Event').PERSIST_STATE,
                event_data=EventPersistStateData(is_migrating=True),
            )

        # Wait for state to be persisted
        await asyncio.sleep(1)

        logger.info('[Scrapely] Rebooting...')

        # Call reboot endpoint
        client = cls.new_client()
        await client.post(f'/v2/actor-runs/{run_id}/reboot')

        # Wait before exiting
        sleep_ms = opts.custom_after_sleep_millis or config.get('metamorph_after_sleep_millis', 1000)
        await asyncio.sleep(sleep_ms / 1000)

        # Flush pending PPE intents before hard exit.
        try:
            await cls._shutdown(timeout_secs=5)
        except Exception as err:
            logger.warning('flush_pending during reboot failed: %s', err)

        logger.info('[Scrapely] Reboot request accepted, exiting...')
        sys.exit(0)

    @classmethod
    async def add_webhook(cls, webhook: Webhook) -> Optional[Webhook]:
        """Create an ad-hoc webhook for the current Actor run.

        This webhook lets you receive a notification when the Actor run
        finished or failed.

        Note that webhooks are only supported for Actors running on the
        Scrapely platform. When running the Actor locally, the function
        will print a warning and return None.

        Args:
            webhook: The webhook to add. It is automatically bound to the
                current Actor run.

        Returns:
            The created webhook, or None if running locally
        """
        cls._ensure_initialized('add_webhook')
        
        if not cls.is_at_home():
            logger.warning('Actor.add_webhook() is only supported on the Scrapely platform. Ignoring.')
            return None

        config = cls.get_config()
        run_id = config.get('actor_run_id')
        if not run_id:
            raise RuntimeError('actor_run_id is not set - add_webhook requires running on the platform')

        client = cls.new_client()
        
        # Build webhook creation request
        body = {
            'eventTypes': webhook.event_types,
            'requestUrl': webhook.request_url,
            'isAdHoc': True,
            'condition': {'actorRunId': run_id},
        }
        if webhook.payload_template:
            body['payloadTemplate'] = webhook.payload_template
        if webhook.idempotency_key:
            body['idempotencyKey'] = webhook.idempotency_key
        if webhook.headers_template:
            body['headersTemplate'] = webhook.headers_template
        # `description` is not part of the Webhook dataclass — tolerate future additions.
        description = getattr(webhook, 'description', None)
        if description:
            body['description'] = description
        body['ignoreSslErrors'] = bool(webhook.ignore_ssl_errors)
        body['doNotRetry'] = bool(webhook.do_not_retry)

        data = _payload_dict(await client.post_json('/v2/webhooks', data=body))

        # Populate the server-assigned ID on the returned webhook so callers
        # can reference it later (Node SDK parity).
        webhook.id = data.get('id') or webhook.id

        return webhook

    @classmethod
    async def use_state(
        cls,
        default_value: Optional[Dict[str, Any]] = None,
        key: Optional[str] = None,
        kvs_name: Optional[str] = None,
    ) -> MutableMapping[str, Any]:
        """Easily create and manage state values. All state values are automatically persisted.

        Values can be modified by simply using the assignment operator.

        Args:
            default_value: The default value to initialize the state if not already set
            key: The key in the key-value store where state is stored. Defaults to 'APIFY_GLOBAL_STATE'
            kvs_name: The name of the key-value store where state is stored. Defaults to default KVS.

        Returns:
            The state dictionary with automatic persistence
        """
        cls._ensure_initialized('use_state')
        
        state_key = key or 'APIFY_GLOBAL_STATE'
        initial_value = default_value or {}
        
        cls._use_state_stores.add(kvs_name)
        kvs = await cls.open_key_value_store(name=kvs_name)
        
        # Use get_auto_saved_value if available (from patch_storage)
        if hasattr(kvs, 'get_auto_saved_value'):
            return await kvs.get_auto_saved_value(state_key, initial_value)
        
        # Fallback: manual state management
        existing = await kvs.get_value(state_key)
        if existing is None:
            await kvs.set_value(state_key, initial_value)
            return initial_value
        return cast(MutableMapping[str, Any], existing)

    # -------------------------------------------------------------------------
    # Remote browser configuration
    # -------------------------------------------------------------------------

    @classmethod
    async def create_browser_configuration(
        cls,
        actor_browser_input: Optional[Dict[str, Any]] = None,
        *,
        country_code: Optional[str] = None,
        state: Optional[str] = None,
        city: Optional[str] = None,
        os: Optional[str] = None,
        timezone: Optional[str] = None,
        language: Optional[str] = None,
        adblock: Optional[bool] = None,
        solve_cloudflare: Optional[bool] = None,
    ) -> 'BrowserConfiguration':
        """Create a Scrapely remote browser configuration (location and fingerprint options).

        Pass it to browser crawlers next to ``proxy_configuration``::

            browser_configuration = await Actor.create_browser_configuration(country_code='US', os='windows')
            crawler = PlaywrightCrawler(browser_configuration=browser_configuration, request_handler=handler)

        Ignored outside the Scrapely remote browser (local runs), so the same
        code runs locally and on the platform.

        Args:
            actor_browser_input: Actor input fields ``browserCountry`` /
                ``browserState`` / ``browserCity``, used when the matching
                keyword argument is not given.
            country_code: Exit country (ISO 3166-1 alpha-2).
            state: Region name (e.g. ``California``).
            city: City name (e.g. ``San Jose``).
            os: Fingerprint OS: ``windows``, ``macos`` or ``android``.
            timezone: IANA timezone.
            language: Comma-separated language tags without q-values.
            adblock: Built-in ad/tracker blocking.
            solve_cloudflare: Automatic Cloudflare Turnstile solving.
        """
        from .browser_configuration import BrowserConfiguration

        if actor_browser_input:
            country_code = country_code if country_code is not None else actor_browser_input.get('browserCountry')
            state = state if state is not None else actor_browser_input.get('browserState')
            city = city if city is not None else actor_browser_input.get('browserCity')

        return BrowserConfiguration(
            country_code=country_code,
            state=state,
            city=city,
            os=os,
            timezone=timezone,
            language=language,
            adblock=adblock,
            solve_cloudflare=solve_cloudflare,
        )

    # -------------------------------------------------------------------------
    # Proxy configuration
    # -------------------------------------------------------------------------

    @classmethod
    async def create_proxy_configuration(
        cls,
        actor_proxy_input: Optional[Dict[str, Any]] = None,
        password: Optional[str] = None,
        groups: Optional[List[str]] = None,
        country_code: Optional[str] = None,
        subdivision_code: Optional[str] = None,
        proxy_urls: Optional[List[str]] = None,
        new_url_function: Optional[Callable] = None,
        tiered_proxy_urls: Optional[List[List[Optional[str]]]] = None,
        tiered_proxy_config: Optional[List[Dict[str, Any]]] = None,
        check_access: bool = True,
    ) -> Optional['ProxyConfiguration']:
        """Create a ProxyConfiguration object with the passed proxy configuration.

        Configures connection to a proxy server with the provided options.
        Proxy servers are used to prevent target websites from blocking your
        crawlers based on IP address rate limits or blacklists.

        Args:
            actor_proxy_input: Proxy configuration field from Actor input
            password: Password for the proxy
            groups: Proxy groups to use
            country_code: Country code for proxy
            subdivision_code: Subdivision code for proxy
            proxy_urls: Custom proxy server URLs to rotate through; may contain
                the 'auto' / 'http://auto' sentinel to mix in the platform proxy
            new_url_function: Function that returns a custom proxy URL
            tiered_proxy_urls: Proxy URL lists stratified into tiers
            tiered_proxy_config: Declarative tiers (proxy_urls / groups / ...)
            check_access: Whether to run the best-effort platform status check

        Returns:
            ProxyConfiguration or None if no proxy should be used
        """
        cls._ensure_initialized('create_proxy_configuration')

        from .proxy_configuration import ProxyConfiguration, _input_proxy_option, _input_use_platform_proxy

        # Handle actor_proxy_input (Node SDK normalizeProxyInput/createProxyConfiguration
        # parity): `useScrapelyProxy: false` (or the legacy `useApifyProxy: false`; a
        # boolean useScrapelyProxy wins) explicitly disables the PLATFORM proxy only —
        # custom proxies (proxyUrls / tiered lists / new_url_function) from the same
        # input must still be used. The standard "Custom proxy URLs" input shape is
        # {useScrapelyProxy: false, proxyUrls: [...]}; returning None for it would
        # silently drop the user's own proxies and crawl on their real IPs. We only
        # bail when no custom proxies are given either. A MISSING key falls back to
        # platform mode when the server injected credentials — input like
        # {scrapelyProxyGroups: [...]} must not silently disable proxying.
        if actor_proxy_input is not None:
            if _input_use_platform_proxy(actor_proxy_input) is False and not (
                proxy_urls
                or new_url_function
                or tiered_proxy_urls
                or tiered_proxy_config
                or actor_proxy_input.get('proxyUrls')
                or actor_proxy_input.get('tieredProxyUrls')
                or actor_proxy_input.get('tieredProxyConfig')
            ):
                return None
            country_code = country_code or _input_proxy_option(actor_proxy_input, 'Country')
            subdivision_code = subdivision_code or _input_proxy_option(actor_proxy_input, 'Subdivision')
            groups = groups or _input_proxy_option(actor_proxy_input, 'Groups')
            proxy_urls = proxy_urls or actor_proxy_input.get('proxyUrls')
            tiered_proxy_urls = tiered_proxy_urls or actor_proxy_input.get('tieredProxyUrls')
            tiered_proxy_config = tiered_proxy_config or actor_proxy_input.get('tieredProxyConfig')

        # No custom proxies configured: use the platform proxy only when the
        # server injected credentials; locally, warn and run proxy-less (Node
        # SDK createProxyConfiguration parity).
        if not (proxy_urls or new_url_function or tiered_proxy_urls or tiered_proxy_config):
            from .proxy_configuration import _platform_env
            if not _platform_env():
                logger.warning(
                    'No proxy configured. Scrapely injects platform proxy credentials '
                    '(SCRAPELY_PROXY_*/APIFY_PROXY_*) into runs when your account has '
                    'proxy access; locally, add proxy_urls to your actor input.'
                )
                return None

        config = cls.get_config()
        proxy_config = ProxyConfiguration(
            password=password,
            groups=groups,
            country_code=country_code,
            subdivision_code=subdivision_code,
            proxy_urls=proxy_urls,
            new_url_function=new_url_function,
            tiered_proxy_urls=tiered_proxy_urls,
            tiered_proxy_config=tiered_proxy_config,
            _actor_config=config,
            _api_client=cls.new_client(),
        )

        await proxy_config.initialize(check_access=check_access)
        return proxy_config
