"""
Configuration management for the Scrapely Python SDK.

Handles environment variables and configuration options for Actors.
"""

import json
import os
from datetime import datetime
from typing import Any, Dict, Optional, cast

from .types import ActorPermissionLevel, ApifyEnv



class Configuration:
    """Configuration management for Scrapely Actors.
    
    Handles environment variables and provides default values.
    """
    
    # Environment variable to configuration key mapping
    ENV_MAP = {
        # APIFY_* variables
        'APIFY_TOKEN': 'token',
        'APIFY_API_BASE_URL': 'api_base_url',
        'APIFY_API_PUBLIC_BASE_URL': 'api_public_base_url',
        'APIFY_IS_AT_HOME': 'is_at_home',
        'APIFY_ACTOR_EVENTS_WS_URL': 'actor_events_ws_url',
        'APIFY_ACTOR_ID': 'actor_id',
        'APIFY_ACTOR_RUN_ID': 'actor_run_id',
        'APIFY_ACTOR_TASK_ID': 'actor_task_id',
        'APIFY_CONTAINER_PORT': 'container_port',
        'APIFY_CONTAINER_URL': 'container_url',
        'APIFY_USER_ID': 'user_id',
        'APIFY_USER_IS_PAYING': 'user_is_paying',
        'APIFY_PROXY_HOSTNAME': 'proxy_hostname',
        'APIFY_PROXY_PASSWORD': 'proxy_password',
        'APIFY_PROXY_STATUS_URL': 'proxy_status_url',
        'APIFY_PROXY_PORT': 'proxy_port',
        'APIFY_INPUT_SECRETS_PRIVATE_KEY_FILE': 'input_secrets_private_key_file',
        'APIFY_INPUT_SECRETS_PRIVATE_KEY_PASSPHRASE': 'input_secrets_private_key_passphrase',
        'APIFY_META_ORIGIN': 'meta_origin',
        'APIFY_DEFAULT_DATASET_ID': 'default_dataset_id',
        'APIFY_DEFAULT_KEY_VALUE_STORE_ID': 'default_key_value_store_id',
        'APIFY_DEFAULT_REQUEST_QUEUE_ID': 'default_request_queue_id',
        'APIFY_MEMORY_MBYTES': 'memory_mbytes',
        'APIFY_INPUT_KEY': 'input_key',
        'APIFY_ACTOR_PRICING_INFO': 'actor_pricing_info',
        'APIFY_CHARGED_ACTOR_EVENT_COUNTS': 'charged_event_counts',
        'APIFY_USER_PRICING_TIER': 'user_pricing_tier',
        'APIFY_LOG_LEVEL': 'log_level',
        'APIFY_LOG_FORMAT': 'log_format',
        'APIFY_LOCAL_STORAGE_DIR': 'local_storage_dir',
        'APIFY_HEADLESS': 'headless',
        'APIFY_SDK_LATEST_VERSION': 'sdk_latest_version',
        'APIFY_SYSTEM_INFO_INTERVAL_MILLIS': 'system_info_interval_millis',
        'APIFY_WORKFLOW_KEY': 'workflow_key',
        'APIFY_DEDICATED_CPUS': 'dedicated_cpus',
        'APIFY_DISABLE_OUTDATED_WARNING': 'disable_outdated_warning',
        'APIFY_FACT': 'fact',
        'APIFY_CHROME_EXECUTABLE_PATH': 'chrome_executable_path',
        'APIFY_PURGE_ON_START': 'purge_on_start',
        'APIFY_XVFB': 'xvfb',
        'APIFY_METAMORPH_AFTER_SLEEP_MILLIS': 'metamorph_after_sleep_millis',
        'APIFY_PERSIST_STATE_INTERVAL_MILLIS': 'persist_state_interval_millis',
        'APIFY_ACTOR_BUILD_ID': 'actor_build_id',
        'APIFY_ACTOR_BUILD_NUMBER': 'actor_build_number',
        'APIFY_ACTOR_MAX_PAID_DATASET_ITEMS': 'actor_max_paid_dataset_items',
        'APIFY_STARTED_AT': 'started_at',
        'APIFY_TIMEOUT_AT': 'timeout_at',
        
        # SCRAPELY_* variables

        'SCRAPELY_TOKEN': 'token',
        'SCRAPELY_API_URL': 'api_base_url',
        'SCRAPELY_IS_AT_HOME': 'is_at_home',
        
        # ACTOR_* variables
        'ACTOR_ID': 'actor_id',
        'ACTOR_RUN_ID': 'actor_run_id',
        'ACTOR_TASK_ID': 'actor_task_id',
        'ACTOR_DEFAULT_DATASET_ID': 'default_dataset_id',
        'ACTOR_DEFAULT_KEY_VALUE_STORE_ID': 'default_key_value_store_id',
        'ACTOR_DEFAULT_REQUEST_QUEUE_ID': 'default_request_queue_id',
        'ACTOR_EVENTS_WEBSOCKET_URL': 'actor_events_ws_url',
        'ACTOR_INPUT_KEY': 'input_key',
        'ACTOR_MEMORY_MBYTES': 'memory_mbytes',
        'ACTOR_PERMISSION_LEVEL': 'actor_permission_level',
        'ACTOR_WEB_SERVER_PORT': 'container_port',
        'ACTOR_WEB_SERVER_URL': 'container_url',
        'ACTOR_MAX_TOTAL_CHARGE_USD': 'max_total_charge_usd',
        'ACTOR_TEST_PAY_PER_EVENT': 'test_pay_per_event',
        'ACTOR_USE_CHARGING_LOG_DATASET': 'use_charging_log_dataset',
        'ACTOR_STORAGES_JSON': 'actor_storages_json',
        'ACTOR_META_ORIGIN': 'meta_origin',
        'ACTOR_TIMEOUT_AT': 'timeout_at',
        'ACTOR_STARTED_AT': 'started_at',
        'ACTOR_BUILD_ID': 'actor_build_id',
        'ACTOR_BUILD_NUMBER': 'actor_build_number',
        'ACTOR_BUILD_TAGS': 'actor_build_tags',
        'ACTOR_FULL_NAME': 'actor_full_name',
        'ACTOR_INPUT': 'actor_input',
        'ACTOR_METAMORPH_COUNT': 'metamorph_count',
        'ACTOR_MAX_PAID_DATASET_ITEMS': 'actor_max_paid_dataset_items',
        'ACTOR_RESTART_ON_ERROR': 'restart_on_error',
        'ACTOR_STANDBY_PORT': 'standby_port',
        'ACTOR_STANDBY_URL': 'standby_url',
        
        # CRAWLEE_* variables
        'CRAWLEE_STORAGE_DIR': 'local_storage_dir',
    }

    
    # Variables that should be parsed as integers
    INTEGER_VARS = {
        'container_port', 'proxy_port', 'memory_mbytes',
        'persist_state_interval_millis',
        'metamorph_after_sleep_millis', 'actor_max_paid_dataset_items',
        'standby_port', 'dedicated_cpus', 'disable_outdated_warning',
    }

    # Variables that should be parsed as floats (USD amounts etc.)
    FLOAT_VARS = {
        'max_total_charge_usd',
    }
    
    # Variables that should be parsed as booleans
    BOOLEAN_VARS = {
        'is_at_home', 'test_pay_per_event', 'use_charging_log_dataset',
        'user_is_paying', 'purge_on_start', 'headless', 'restart_on_error',
    }

    
    # Default values
    DEFAULTS = {
        'default_key_value_store_id': 'default',
        'default_dataset_id': 'default',
        'default_request_queue_id': 'default',
        'input_key': 'INPUT',
        'api_base_url': 'https://api.scrape.ly',
        'api_public_base_url': 'https://api.scrape.ly',
        'proxy_status_url': 'http://proxy.scrape.ly',
        'proxy_hostname': 'proxy.scrape.ly',
        'proxy_port': 8000,
        'container_port': 4321,
        # Standby runs listen on ACTOR_STANDBY_PORT (4321 unless set).
        'standby_port': 4321,
        'container_url': 'http://localhost:4321',
        'metamorph_after_sleep_millis': 300000,
        'persist_state_interval_millis': 60000,
        'test_pay_per_event': False,
        'use_charging_log_dataset': False,
    }
    
    _instance: Optional['Configuration'] = None
    
    def __init__(self, options: Optional[Dict[str, Any]] = None):
        """Initialize configuration with optional overrides.
        
        Args:
            options: Configuration options to override defaults
        """
        self._config: Dict[str, Any] = {}
        self._options = options or {}
        
        # Load from environment and options
        self._load_from_env()
        self._load_from_options()
    
    @classmethod
    def get_global_config(cls) -> 'Configuration':
        """Get or create the global configuration instance."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def get_global_configuration(cls) -> 'Configuration':
        """Crawlee-compatible alias for get_global_config()."""
        return cls.get_global_config()

    @classmethod
    def reset_global_state(cls) -> None:
        """Reset the global configuration instance."""
        cls._instance = None
    
    def _load_from_env(self) -> None:
        """Load configuration from environment variables."""
        for env_key, config_key in self.ENV_MAP.items():
            value = os.environ.get(env_key)
            if value is not None:
                self._config[config_key] = self._parse_value(config_key, value)
    
    def _load_from_options(self) -> None:
        """Load configuration from provided options."""
        for key, value in self._options.items():
            if value is not None:
                self._config[key] = value
    
    def _parse_value(self, key: str, value: str) -> Any:
        """Parse a string value based on the key type."""
        if key in self.INTEGER_VARS:
            try:
                return int(value)
            except ValueError:
                return value
        elif key in self.FLOAT_VARS:
            try:
                return float(value)
            except ValueError:
                return value
        elif key in self.BOOLEAN_VARS:
            return value.lower() in ('1', 'true', 'yes')
        elif key in ('timeout_at', 'started_at'):
            try:
                return datetime.fromisoformat(value.replace('Z', '+00:00'))
            except (ValueError, TypeError):
                return value
        return value
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get a configuration value.
        
        Args:
            key: Configuration key
            default: Default value if not found
            
        Returns:
            Configuration value or default
        """
        # Check config first (from env/options)
        if key in self._config:
            return self._config[key]
        # Check defaults
        if key in self.DEFAULTS:
            return self.DEFAULTS[key]
        return default
    
    def set(self, key: str, value: Any) -> None:
        """Set a configuration value.
        
        Args:
            key: Configuration key
            value: Value to set
        """
        self._config[key] = value
    
    def __getattr__(self, name: str) -> Any:
        """Attribute access to any known setting (``Actor.config.<field>``).

        ``Actor.config.standby_port``, ``Actor.config.meta_origin`` and the other
        keys this class reads from the environment resolve like ``get(name)``.
        """
        if name.startswith('_'):
            raise AttributeError(name)
        known = name in self.DEFAULTS or name in self.ENV_MAP.values() or name in self.__dict__.get('_config', {})
        if not known:
            raise AttributeError(f'{type(self).__name__!r} object has no attribute {name!r}')
        return self.get(name)

    @property
    def standby_port(self) -> int:
        """Port a standby run serves HTTP on (ACTOR_STANDBY_PORT, default 4321)."""
        value = self.get('standby_port')
        return value if isinstance(value, int) else 4321

    @property
    def web_server_port(self) -> int:
        """Alias for the container port (ACTOR_WEB_SERVER_PORT, default 4321)."""
        value = self.get('container_port')
        return value if isinstance(value, int) else 4321

    @property
    def web_server_url(self) -> str:
        """Alias for the container URL (ACTOR_WEB_SERVER_URL)."""
        return cast(str, self.get('container_url'))

    @property
    def standby_url(self) -> str:
        """Public standby URL (ACTOR_STANDBY_URL), falling back to the container URL."""
        return cast(str, self.get('standby_url') or self.get('container_url'))

    @property
    def token(self) -> Optional[str]:
        """Get the authentication token."""
        return cast(Optional[str], self.get('token'))
    
    @property
    def api_base_url(self) -> str:
        """Get the API base URL."""
        return cast(str, self.get('api_base_url', self.DEFAULTS['api_base_url']))
    
    @property
    def actor_run_id(self) -> Optional[str]:
        """Get the current Actor run ID."""
        return cast(Optional[str], self.get('actor_run_id'))
    
    @property
    def actor_id(self) -> Optional[str]:
        """Get the current Actor ID."""
        return cast(Optional[str], self.get('actor_id'))
    
    @property
    def is_at_home(self) -> bool:
        """Check if running on the Scrapely platform."""
        return cast(bool, self.get('is_at_home', False))
    
    @property
    def default_dataset_id(self) -> str:
        """Get the default dataset ID."""
        return cast(str, self.get('default_dataset_id', self.DEFAULTS['default_dataset_id']))
    
    @property
    def default_key_value_store_id(self) -> str:
        """Get the default key-value store ID."""
        return cast(str, self.get('default_key_value_store_id', self.DEFAULTS['default_key_value_store_id']))
    
    @property
    def default_request_queue_id(self) -> str:
        """Get the default request queue ID."""
        return cast(str, self.get('default_request_queue_id', self.DEFAULTS['default_request_queue_id']))
    
    @property
    def input_key(self) -> str:
        """Get the input key."""
        return cast(str, self.get('input_key', self.DEFAULTS['input_key']))
    
    def get_env(self) -> ApifyEnv:
        """Get all environment variables as an ApifyEnv object.
        
        Returns:
            ApifyEnv object with all parsed environment variables
        """
        return ApifyEnv(
            actor_id=self.get('actor_id'),
            actor_build_id=self.get('actor_build_id'),
            actor_build_number=self.get('actor_build_number'),
            actor_build_tags=self.get('actor_build_tags'),
            actor_events_ws_url=self.get('actor_events_ws_url'),
            actor_full_name=self.get('actor_full_name'),
            actor_max_paid_dataset_items=self.get('actor_max_paid_dataset_items'),
            actor_permission_level=self._parse_permission_level(self.get('actor_permission_level')),
            actor_pricing_info=(
                json.dumps(self.get('actor_pricing_info'))
                if isinstance(self.get('actor_pricing_info'), dict)
                else self.get('actor_pricing_info')
            ),
            actor_run_id=self.get('actor_run_id'),
            actor_storages_json=self.get('actor_storages_json'),
            actor_task_id=self.get('actor_task_id'),
            api_base_url=self.get('api_base_url'),
            api_public_base_url=self.get('api_public_base_url'),
            charged_event_counts=(
                json.dumps(self.get('charged_event_counts'))
                if isinstance(self.get('charged_event_counts'), dict)
                else self.get('charged_event_counts')
            ),

            chrome_executable_path=self.get('chrome_executable_path'),
            container_port=self.get('container_port'),
            container_url=self.get('container_url'),
            dedicated_cpus=str(self.get('dedicated_cpus')) if self.get('dedicated_cpus') is not None else None,
            default_dataset_id=self.get('default_dataset_id'),
            default_key_value_store_id=self.get('default_key_value_store_id'),
            default_request_queue_id=self.get('default_request_queue_id'),
            disable_outdated_warning=self.get('disable_outdated_warning'),
            fact=self.get('fact'),
            headless='1' if self.get('headless') else None,
            input_key=self.get('input_key'),
            input_secrets_private_key_file=self.get('input_secrets_private_key_file'),
            input_secrets_private_key_passphrase=self.get('input_secrets_private_key_passphrase'),
            is_at_home='1' if self.get('is_at_home') else None,
            local_storage_dir=self.get('local_storage_dir'),
            log_format=self.get('log_format'),
            log_level=self.get('log_level'),
            memory_mbytes=self.get('memory_mbytes'),
            meta_origin=self.get('meta_origin'),
            metamorph_after_sleep_millis=self.get('metamorph_after_sleep_millis'),
            persist_state_interval_millis=self.get('persist_state_interval_millis'),
            proxy_hostname=self.get('proxy_hostname'),
            proxy_password=self.get('proxy_password'),
            proxy_port=str(self.get('proxy_port')) if self.get('proxy_port') is not None else None,
            proxy_status_url=self.get('proxy_status_url'),
            purge_on_start='1' if self.get('purge_on_start') else None,
            sdk_latest_version=self.get('sdk_latest_version'),
            started_at=self.get('started_at'),
            standby_port=self.get('standby_port'),
            standby_url=self.get('standby_url'),
            system_info_interval_millis=self.get('system_info_interval_millis'),
            timeout_at=self.get('timeout_at'),
            token=self.get('token'),
            user_id=self.get('user_id'),
            user_is_paying='1' if self.get('user_is_paying') else None,
            user_pricing_tier=self.get('user_pricing_tier'),
            workflow_key=self.get('workflow_key'),
            xvfb=self.get('xvfb'),
            max_total_charge_usd=self.get('max_total_charge_usd'),
            user_plan=self.get('user_plan'),
            restart_on_error=self.get('restart_on_error'),
        )

    
    def _parse_permission_level(self, value: Optional[str]) -> Optional[ActorPermissionLevel]:
        """Parse permission level string to enum."""
        if value is None:
            return None
        try:
            return ActorPermissionLevel(value.upper())
        except ValueError:
            return None