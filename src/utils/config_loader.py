"""Simple configuration loader for Market Risk Analytics."""

import yaml
from pathlib import Path
from typing import Dict, Any


def load_config(config_path: str = None) -> Dict[str, Any]:
    """
    Load configuration from YAML file.

    Args:
        config_path: Path to config file. If None, uses default location.

    Returns:
        Dictionary containing configuration

    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If config is invalid
    """

    if config_path is None:
        # Default to config/config.yaml relative to project root
        project_root = Path(__file__).parent.parent.parent
        config_path = project_root / "config" / "config.yaml"
    else:
        config_path = Path(config_path)

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    # Validate required fields
    if 'currency_pairs' not in config:
        raise ValueError("Config must contain 'currency_pairs' section")

    if not config['currency_pairs']:
        raise ValueError("'currency_pairs' section cannot be empty")

    # Validate that all currency pairs have base sizes
    for pair, size in config['currency_pairs'].items():
        if not isinstance(size, (int, float)) or size <= 0:
            raise ValueError(
                f"Invalid base size for {pair}: {size}. Must be positive number."
            )
    return config

def get_currency_pairs(config: Dict[str, Any]) -> list:
    """
    Get list of currency pairs from config.

    Args:
        config: Configuration dictionary

    Returns:
        List of currency pair strings (e.g., ['EURUSD', 'GBPUSD', ...])
    """
    return list(config['currency_pairs'].keys())

def get_base_sizes(config: Dict[str, Any]) -> Dict[str, float]:
    """
    Get base position sizes for each currency pair.

    Args:
        config: Configuration dictionary

    Returns:
        Dictionary mapping currency pair to base size
    """
    return config['currency_pairs']

# For easy notebook usage
def quick_load():
    """Quick load with defaults - for notebook convenience."""

    config = load_config()

    return {
        'config': config,
        'currency_pairs': get_currency_pairs(config),
        'base_sizes': get_base_sizes(config)
    }