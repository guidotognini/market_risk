"""
Configuration Loader for FX Market Risk Pipeline

This module provides a centralized configuration management system for the FX Market Risk pipeline.
It implements the Job as Code pattern by loading YAML configuration files and merging
environment-specific overrides.

Usage:
    # In Databricks notebooks
    from config_loader import PipelineConfig

    config = PipelineConfig(environment='prod')

    # Access configuration values
    currency_pairs = config.get_currency_pairs()
    catalog = config.catalog
    schema = config.schema
    confidence_level = config.var_confidence_level

    # Get SQL parameters for DLT queries
    sql_params = config.get_sql_parameters()
"""

import os
import yaml
from typing import Dict, List, Any, Optional
from pathlib import Path


class ConfigurationError(Exception):
    """Raised when configuration is invalid or missing required fields."""
    pass


class PipelineConfig:
    """
    Centralized configuration manager for FX Market Risk pipeline.

    Loads base configuration from pipeline_config.yaml and merges with
    environment-specific overrides from dev.yaml or prod.yaml.
    """

    def __init__(self, environment: str = 'dev', config_dir: Optional[str] = None):
        """
        Initialize configuration loader.

        Args:
            environment: Target environment ('dev' or 'prod')
            config_dir: Path to config directory (defaults to ../config relative to this file)
        """
        self.environment = environment.lower()

        # Determine config directory
        if config_dir is None:
            # Default: config/ directory at project root
            current_file = Path(__file__).resolve()
            self.config_dir = current_file.parent.parent / 'config'
        else:
            self.config_dir = Path(config_dir)

        # Validate environment
        if self.environment not in ['dev', 'prod']:
            raise ConfigurationError(
                f"Invalid environment: {self.environment}. Must be 'dev' or 'prod'"
            )

        # Load and merge configurations
        self._base_config = self._load_yaml('pipeline_config.yaml')
        self._env_config = self._load_yaml(f'{self.environment}.yaml')
        self._config = self._deep_merge(self._base_config, self._env_config)

        # Validate required fields
        self._validate()

    def _load_yaml(self, filename: str) -> Dict[str, Any]:
        """Load YAML configuration file."""
        filepath = self.config_dir / filename

        if not filepath.exists():
            raise ConfigurationError(f"Configuration file not found: {filepath}")

        try:
            with open(filepath, 'r') as f:
                config = yaml.safe_load(f)
                return config if config else {}
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Error parsing {filename}: {str(e)}")

    def _deep_merge(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """
        Deep merge two dictionaries, with override taking precedence.

        Args:
            base: Base configuration dictionary
            override: Override configuration dictionary

        Returns:
            Merged configuration dictionary
        """
        result = base.copy()

        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value

        return result

    def _validate(self):
        """Validate that required configuration fields are present."""
        required_fields = [
            ('databricks', 'catalog'),
            ('databricks', 'schema'),
            ('storage', 'base_path'),
            ('currencies', 'pairs'),
            ('var_parameters', 'confidence_level'),
        ]

        for *path, field in required_fields:
            config = self._config
            for key in path:
                if key not in config:
                    raise ConfigurationError(f"Missing required configuration: {'.'.join(path)}.{field}")
                config = config[key]

            if field not in config:
                raise ConfigurationError(f"Missing required configuration: {'.'.join(path)}.{field}")

        # Validate currency pairs structure
        if not isinstance(self._config['currencies']['pairs'], list):
            raise ConfigurationError("currencies.pairs must be a list")

        if len(self._config['currencies']['pairs']) == 0:
            raise ConfigurationError("currencies.pairs cannot be empty")

    # ========================================================================
    # Property Accessors - Databricks Configuration
    # ========================================================================

    @property
    def catalog(self) -> str:
        """Get Databricks catalog name."""
        return self._config['databricks']['catalog']

    @property
    def schema(self) -> str:
        """Get Databricks schema name."""
        return self._config['databricks']['schema']

    @property
    def username(self) -> str:
        """Get Databricks username."""
        return self._config['databricks']['username']

    @property
    def fully_qualified_schema(self) -> str:
        """Get fully qualified schema name (catalog.schema)."""
        return f"{self.catalog}.{self.schema}"

    # ========================================================================
    # Property Accessors - Storage Paths
    # ========================================================================

    @property
    def base_path(self) -> str:
        """Get base storage path."""
        return self._config['storage']['base_path']

    @property
    def fx_rates_raw_path(self) -> str:
        """Get raw FX rates storage path."""
        return self._config['storage']['raw_data']['fx_rates']

    @property
    def positions_raw_path(self) -> str:
        """Get raw positions storage path."""
        return self._config['storage']['raw_data']['positions']

    @property
    def fx_rates_schema_path(self) -> str:
        """Get FX rates schema storage path."""
        return self._config['storage']['schemas']['fx_rates']

    @property
    def positions_schema_path(self) -> str:
        """Get positions schema storage path."""
        return self._config['storage']['schemas']['positions']

    # ========================================================================
    # Property Accessors - VaR Parameters
    # ========================================================================

    @property
    def var_confidence_level(self) -> float:
        """Get VaR confidence level (e.g., 0.95 for 95%)."""
        return self._config['var_parameters']['confidence_level']

    @property
    def var_percentile_long(self) -> float:
        """Get percentile for LONG positions (downside risk)."""
        return self._config['var_parameters']['percentile_long']

    @property
    def var_percentile_short(self) -> float:
        """Get percentile for SHORT positions (upside risk)."""
        return self._config['var_parameters']['percentile_short']

    @property
    def lookback_days(self) -> int:
        """Get lookback window for VaR calculation."""
        return self._config['var_parameters']['lookback_days']

    @property
    def lookback_interval_days(self) -> int:
        """Get lookback interval for data fetching."""
        return self._config['var_parameters']['lookback_interval_days']

    @property
    def minimum_data_date(self) -> str:
        """Get minimum date filter for VaR calculations."""
        return self._config['var_parameters']['minimum_data_date']

    @property
    def return_anomaly_threshold(self) -> float:
        """Get threshold for flagging anomalous returns."""
        return self._config['var_parameters']['return_anomaly_threshold']

    # ========================================================================
    # Property Accessors - Pipeline Metadata
    # ========================================================================

    @property
    def pipeline_name(self) -> str:
        """Get pipeline name."""
        return self._config['pipeline']['name']

    @property
    def pipeline_timezone(self) -> str:
        """Get pipeline timezone."""
        return self._config['pipeline']['timezone']

    @property
    def pipeline_version(self) -> str:
        """Get pipeline version."""
        return self._config['pipeline']['version']

    # ========================================================================
    # Property Accessors - API Configuration
    # ========================================================================

    @property
    def polygon_secret_scope(self) -> str:
        """Get Polygon API secret scope."""
        return self._config['api']['polygon']['secret_scope']

    @property
    def polygon_secret_key(self) -> str:
        """Get Polygon API secret key name."""
        return self._config['api']['polygon']['secret_key']

    @property
    def polygon_base_url(self) -> str:
        """Get Polygon API base URL."""
        return self._config['api']['polygon']['base_url']

    # ========================================================================
    # Currency Pairs Methods
    # ========================================================================

    def get_currency_pairs(self) -> List[Dict[str, Any]]:
        """
        Get list of all currency pairs with metadata.

        Returns:
            List of currency pair dictionaries with keys:
            - symbol (e.g., 'EURUSD')
            - name (e.g., 'EUR/USD')
            - base_currency (e.g., 'EUR')
            - quote_currency (e.g., 'USD')
            - liquidity_tier (e.g., 'tier1')
            - base_position_size (e.g., 15000000)
            - is_usd_quote (e.g., True)
        """
        return self._config['currencies']['pairs']

    def get_currency_symbols(self) -> List[str]:
        """
        Get list of currency pair symbols only.

        Returns:
            List of symbols (e.g., ['EURUSD', 'GBPUSD', ...])
        """
        return [pair['symbol'] for pair in self.get_currency_pairs()]

    def get_currency_names(self) -> List[str]:
        """
        Get list of currency pair names (formatted).

        Returns:
            List of names (e.g., ['EUR/USD', 'GBP/USD', ...])
        """
        return [pair['name'] for pair in self.get_currency_pairs()]

    def get_base_positions(self) -> Dict[str, float]:
        """
        Get base position sizes for position generation.

        Returns:
            Dictionary mapping currency pair name to base position size
            Example: {'EUR/USD': 15000000, 'GBP/USD': 8000000, ...}
        """
        return {
            pair['name']: pair['base_position_size']
            for pair in self.get_currency_pairs()
        }

    def get_currency_pair(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get currency pair configuration by symbol.

        Args:
            symbol: Currency pair symbol (e.g., 'EURUSD')

        Returns:
            Currency pair dictionary or None if not found
        """
        for pair in self.get_currency_pairs():
            if pair['symbol'] == symbol:
                return pair
        return None

    # ========================================================================
    # Position Generation Configuration
    # ========================================================================

    @property
    def desk_name(self) -> str:
        """Get trading desk name."""
        return self._config['position_generation']['desk']

    @property
    def long_probability(self) -> float:
        """Get probability of LONG position."""
        return self._config['position_generation']['direction_bias']['long_probability']

    @property
    def short_probability(self) -> float:
        """Get probability of SHORT position."""
        return self._config['position_generation']['direction_bias']['short_probability']

    @property
    def flat_probability(self) -> float:
        """Get probability of FLAT (no position)."""
        return self._config['position_generation']['flat_probability']

    @property
    def position_max_deviation(self) -> float:
        """Get maximum random walk deviation for position sizing."""
        return self._config['position_generation']['random_walk']['max_deviation']

    # ========================================================================
    # Table Names
    # ========================================================================

    def get_table_name(self, layer: str, table: str) -> str:
        """
        Get fully qualified table name.

        Args:
            layer: Layer name ('bronze', 'silver', 'gold')
            table: Table key (e.g., 'fx_rates', 'positions', 'var')

        Returns:
            Fully qualified table name (catalog.schema.table)
        """
        table_name = self._config['layers'][layer][table]
        return f"{self.catalog}.{self.schema}.{table_name}"

    # ========================================================================
    # SQL Parameters for DLT
    # ========================================================================

    def get_sql_parameters(self) -> Dict[str, Any]:
        """
        Get all parameters needed for SQL queries.

        Returns:
            Dictionary of SQL parameters that can be used in DLT queries
        """
        return {
            # Databricks configuration
            'catalog': self.catalog,
            'schema': self.schema,
            'fully_qualified_schema': self.fully_qualified_schema,

            # Storage paths
            'fx_rates_raw_path': self.fx_rates_raw_path,
            'positions_raw_path': self.positions_raw_path,
            'fx_rates_schema_path': self.fx_rates_schema_path,
            'positions_schema_path': self.positions_schema_path,

            # VaR parameters
            'confidence_level': self.var_confidence_level,
            'percentile_long': self.var_percentile_long,
            'percentile_short': self.var_percentile_short,
            'lookback_days': self.lookback_days,
            'lookback_interval_days': self.lookback_interval_days,
            'minimum_data_date': self.minimum_data_date,
            'return_anomaly_threshold': self.return_anomaly_threshold,

            # Table names
            'bronze_fx_rates': self._config['layers']['bronze']['fx_rates'],
            'bronze_positions': self._config['layers']['bronze']['positions'],
            'silver_fx_rates': self._config['layers']['silver']['fx_rates'],
            'silver_fx_rates_staging': self._config['layers']['silver']['fx_rates_staging'],
            'silver_fx_returns': self._config['layers']['silver']['fx_returns'],
            'silver_positions': self._config['layers']['silver']['positions'],
            'gold_var': self._config['layers']['gold']['var'],
        }

    # ========================================================================
    # Utility Methods
    # ========================================================================

    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation.

        Args:
            key_path: Dot-separated path (e.g., 'databricks.catalog')
            default: Default value if key not found

        Returns:
            Configuration value or default
        """
        keys = key_path.split('.')
        value = self._config

        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default

        return value

    def to_dict(self) -> Dict[str, Any]:
        """
        Get full merged configuration as dictionary.

        Returns:
            Complete configuration dictionary
        """
        return self._config.copy()

    def __repr__(self) -> str:
        """String representation."""
        return f"PipelineConfig(environment='{self.environment}', catalog='{self.catalog}', schema='{self.schema}')"


# ============================================================================
# Helper Functions for Databricks Notebooks
# ============================================================================

def load_config(environment: str = None) -> PipelineConfig:
    """
    Load pipeline configuration with automatic environment detection.

    Args:
        environment: Target environment ('dev' or 'prod').
                    If None, will try to detect from Databricks widgets or default to 'dev'

    Returns:
        PipelineConfig instance
    """
    if environment is None:
        # Try to detect environment from Databricks widget
        try:
            # This will work in Databricks notebooks
            environment = dbutils.widgets.get('environment')  # noqa: F821
        except:
            # Default to dev if not in Databricks or widget not found
            environment = 'dev'

    return PipelineConfig(environment=environment)


def get_config_value(key_path: str, environment: str = 'dev', default: Any = None) -> Any:
    """
    Quick helper to get a single config value.

    Args:
        key_path: Dot-separated path (e.g., 'databricks.catalog')
        environment: Target environment ('dev' or 'prod')
        default: Default value if key not found

    Returns:
        Configuration value or default
    """
    config = PipelineConfig(environment=environment)
    return config.get(key_path, default)
