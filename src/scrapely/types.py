"""
Type definitions for the Scrapely Python SDK.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union

# Single source of truth for ChargeResult lives in pricing_types (pydantic model
# used by ChargingManager); re-exported here for backwards compatibility.
from .pricing_types import ChargeResult  # noqa: F401  (re-export)


class ActorPermissionLevel(str, Enum):
    """Actor permission level (ACTOR_PERMISSION_LEVEL)."""
    NONE = 'NONE'
    RUN = 'RUN'
    WRITE = 'WRITE'
    ADMIN = 'ADMIN'


@dataclass
class ActorInput:
    """Value types returned by Actor.get_input()."""
    data: Dict[str, Any] = field(default_factory=dict)
    
    def get(self, key: str, default: Any = None) -> Any:
        return self.data.get(key, default)
    
    def __getitem__(self, key: str) -> Any:
        return self.data[key]
    
    def __contains__(self, key: str) -> bool:
        return key in self.data


@dataclass
class ApifyEnv:
    """Parsed representation of the environment variables.
    
    This object is returned by the Actor.get_env() function.
    
    Supports both attribute access (env.memory_mbytes) and
    dictionary-style subscript access (env['memory_mbytes']).
    """
    actor_id: Optional[str] = None
    actor_build_id: Optional[str] = None
    actor_build_number: Optional[str] = None
    actor_build_tags: Optional[str] = None
    actor_events_ws_url: Optional[str] = None
    actor_full_name: Optional[str] = None
    actor_max_paid_dataset_items: Optional[int] = None
    actor_permission_level: Optional[ActorPermissionLevel] = None
    actor_pricing_info: Optional[str] = None
    actor_run_id: Optional[str] = None
    actor_storages_json: Optional[str] = None
    actor_task_id: Optional[str] = None
    api_base_url: Optional[str] = None
    api_public_base_url: Optional[str] = None
    charged_event_counts: Optional[str] = None
    chrome_executable_path: Optional[str] = None
    container_port: Optional[int] = None
    container_url: Optional[str] = None
    dedicated_cpus: Optional[str] = None
    default_dataset_id: Optional[str] = None
    default_key_value_store_id: Optional[str] = None
    default_request_queue_id: Optional[str] = None
    disable_outdated_warning: Optional[int] = None
    fact: Optional[str] = None
    headless: Optional[str] = None
    input_key: Optional[str] = None
    input_secrets_private_key_file: Optional[str] = None
    input_secrets_private_key_passphrase: Optional[str] = None
    is_at_home: Optional[str] = None
    local_storage_dir: Optional[str] = None
    log_format: Optional[str] = None
    log_level: Optional[str] = None
    memory_mbytes: Optional[int] = None
    meta_origin: Optional[str] = None
    metamorph_after_sleep_millis: Optional[int] = None
    persist_state_interval_millis: Optional[int] = None
    proxy_hostname: Optional[str] = None
    proxy_password: Optional[str] = None
    proxy_port: Optional[str] = None
    proxy_status_url: Optional[str] = None
    purge_on_start: Optional[str] = None
    sdk_latest_version: Optional[str] = None
    started_at: Optional[datetime] = None
    standby_port: Optional[int] = None
    standby_url: Optional[str] = None
    system_info_interval_millis: Optional[str] = None
    timeout_at: Optional[datetime] = None
    token: Optional[str] = None
    user_id: Optional[str] = None
    user_is_paying: Optional[str] = None
    user_pricing_tier: Optional[str] = None
    workflow_key: Optional[str] = None
    xvfb: Optional[str] = None
    # Scrapely extensions / ACTOR_* extras
    max_total_charge_usd: Optional[float] = None
    user_plan: Optional[str] = None
    restart_on_error: Optional[str] = None


    def __getitem__(self, key: str) -> Any:
        """Enable dictionary-style access: env['memory_mbytes']."""
        return getattr(self, key)

    def __setitem__(self, key: str, value: Any) -> None:
        """Enable dictionary-style assignment: env['memory_mbytes'] = 1024."""
        setattr(self, key, value)

    def keys(self) -> List[str]:
        """Return all field names that have values set (non-None)."""
        return [f.name for f in self.__dataclass_fields__.values() 
                if getattr(self, f.name) is not None]


@dataclass
class WebhookOptions:
    """Options for Actor.add_webhook()."""
    event_types: List[str] = field(default_factory=list)
    request_url: str = ''
    payload_template: Optional[str] = None
    idempotency_key: Optional[str] = None
    headers_template: Optional[str] = None
    description: Optional[str] = None
    ignore_ssl_errors: bool = False
    do_not_retry: bool = False
    should_interpolate_strings: bool = False
    is_apify_integration: bool = False


@dataclass
class InitOptions:
    """Options for Actor.init()."""
    configuration: Optional[Dict[str, Any]] = None
    # ApiStorageConfig dict or a Crawlee StorageClient (tests / custom)
    storage: Optional[Any] = None
    graceful_shutdown: bool = True
    graceful_shutdown_delay_millis: int = 0
    # Set up the default log output (Actor(configure_logging=True))
    configure_logging: bool = True


@dataclass
class ExitOptions:
    """Options for Actor.exit()."""
    status_message: Optional[str] = None
    timeout_secs: int = 30
    exit_code: int = 0
    exit: bool = True


@dataclass
class RecordOptions:
    """Options for Actor.set_value() / KeyValueStore.set_value()."""
    content_type: Optional[str] = None
    do_not_retry_timeouts: bool = False
    timeout_secs: Optional[int] = None


@dataclass
class SetStatusMessageOptions:
    """Options for Actor.set_status_message()."""
    is_status_message_terminal: bool = False
    level: str = 'INFO'  # 'DEBUG', 'INFO', 'WARNING', 'ERROR'


@dataclass
class MetamorphOptions:
    """Options for Actor.metamorph()."""
    build: Optional[str] = None


@dataclass
class RebootOptions:
    """Options for Actor.reboot()."""
    custom_after_sleep_millis: Optional[int] = None


@dataclass
class OpenStorageOptions:
    """Options for Actor.open_dataset(), open_key_value_store(), and open_request_queue()."""
    force_cloud: bool = False
    config: Optional[Any] = None


@dataclass
class CallOptions:
    """Options for Actor.call() / Actor.call_task()."""
    token: Optional[str] = None
    build: Optional[str] = None
    max_total_charge_usd: Optional[float] = None
    restart_on_error: Optional[bool] = None
    memory_mbytes: Optional[int] = None
    timeout: Optional[Union[int, str]] = None  # number or 'inherit'
    force_permission_level: Optional[ActorPermissionLevel] = None
    webhooks: Optional[List[WebhookOptions]] = None


@dataclass
class StartOptions:
    """Options for Actor.start()."""
    token: Optional[str] = None
    build: Optional[str] = None
    max_total_charge_usd: Optional[float] = None
    restart_on_error: Optional[bool] = None
    memory_mbytes: Optional[int] = None
    timeout: Optional[Union[int, str]] = None  # number or 'inherit'
    force_permission_level: Optional[ActorPermissionLevel] = None
    webhooks: Optional[List[WebhookOptions]] = None


@dataclass
class AbortOptions:
    """Options for Actor.abort()."""
    token: Optional[str] = None
    status_message: Optional[str] = None
    gracefully: Optional[bool] = None


@dataclass
class ActorRun:
    """Information about an Actor run."""
    id: str
    actor_id: str
    status: str
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    duration_millis: Optional[int] = None
    build_id: Optional[str] = None
    build_number: Optional[str] = None
    default_dataset_id: Optional[str] = None
    default_key_value_store_id: Optional[str] = None
    default_request_queue_id: Optional[str] = None
    exit_code: Optional[int] = None
    status_message: Optional[str] = None
    input: Optional[Dict[str, Any]] = None
    output: Optional[Dict[str, Any]] = None
    usage_total_usd: Optional[float] = None
    usage: Optional[Dict[str, Any]] = None


@dataclass
class ProxyInfo:
    """Information about a proxy configuration.
    
    Compatible with crawlee's ProxyInfo to support crawlee's _basic_crawler.py
    which calls proxy_configuration.new_proxy_info(request=..., session=...).
    """
    url: str
    scheme: str  # Required by crawlee
    hostname: str
    port: int
    username: str = ''
    password: str = ''
    session_id: Optional[str] = None  # Required by crawlee
    proxy_tier: Optional[int] = None  # Required by crawlee
    groups: List[str] = field(default_factory=list)
    country_code: Optional[str] = None
    subdivision_code: Optional[str] = None


# Type aliases
Dictionary = Dict[str, Any]
JsonValue = Union[None, bool, int, float, str, List['JsonValue'], Dict[str, 'JsonValue']]