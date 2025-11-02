# Databricks notebook source
# MAGIC %md
# MAGIC # Daily Trading Positions Generator
# MAGIC
# MAGIC Generates simulated daily trading positions for FX currency pairs.
# MAGIC
# MAGIC **Configuration-driven:** All parameters loaded from YAML config files.
# MAGIC
# MAGIC **Position Generation Strategy:**
# MAGIC - Random walk: positions vary ±20% from base size daily
# MAGIC - Direction bias: 70% LONG, 30% SHORT (reflects typical bank positions)
# MAGIC - Flat positions: 5% probability of zero position
# MAGIC - Liquidity-aware: base position sizes vary by currency pair tier

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

import sys
from pathlib import Path

# Add src directory to path for imports
project_root = Path("/Workspace/Repos/guidotognini/market_risk")
sys.path.insert(0, str(project_root))

from src.config_loader import load_config
import random
from datetime import datetime
import pandas as pd
import json
import pytz

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Configuration

# COMMAND ----------

# Create widget for environment selection
dbutils.widgets.dropdown("environment", "prod", ["dev", "prod"], "Environment")

# Load configuration
environment = dbutils.widgets.get("environment")
config = load_config(environment=environment)

print(f"🚀 Position Generator - {environment.upper()} Environment")
print(f"📊 Catalog: {config.catalog}")
print(f"📂 Schema: {config.schema}")
print(f"💱 Currency Pairs: {', '.join(config.get_currency_names())}")
print(f"🕐 Timezone: {config.pipeline_timezone}")
print(f"🏢 Desk: {config.desk_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Position Generation Logic

# COMMAND ----------

def generate_daily_positions(date, config):
    """
    Generate simulated daily trading positions using configuration.

    Strategy:
    - Random walk: positions vary by configured max_deviation from base size
    - Direction bias: configured probabilities for LONG/SHORT
    - Flat positions: configured probability of zero position
    - Liquidity-aware: base position sizes from config

    Args:
        date: Date for position generation
        config: PipelineConfig instance with all parameters

    Returns:
        DataFrame with columns: date, currency_pair, position_size, direction, desk, generated_at
    """

    positions = []

    # Get base position sizes from config
    base_positions = config.get_base_positions()

    # Get position generation parameters
    max_deviation = config.position_max_deviation
    long_prob = config.long_probability
    flat_prob = config.flat_probability
    desk = config.desk_name

    for currency_pair, base_size in base_positions.items():
        # Random walk: +/- max_deviation from base size
        daily_change = random.uniform(-max_deviation, max_deviation)
        position_size = base_size * (1 + daily_change)

        # Determine direction based on configured probabilities
        rand_val = random.random()

        # Check for flat position first (5% probability)
        if rand_val < flat_prob:
            position_size = 0
            direction = 'FLAT'
        # Then check for LONG (70% of remaining)
        elif rand_val < (flat_prob + long_prob):
            direction = 'LONG'
        # Otherwise SHORT (30% of remaining)
        else:
            direction = 'SHORT'

        positions.append({
            'date': date,
            'currency_pair': currency_pair,
            'position_size': round(position_size, 2),
            'direction': direction,
            'desk': desk,
            'generated_at': datetime.now()
        })

    return pd.DataFrame(positions)

print("✅ Position generation function defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Today's Positions

# COMMAND ----------

# Get current date in configured timezone
tz = pytz.timezone(config.pipeline_timezone)
today = datetime.now(tz).date()

print(f"📅 Generating positions for: {today}")

# Generate positions
positions_df = generate_daily_positions(today, config)

print(f"✅ Generated {len(positions_df)} positions")
print("\n📊 Position Summary:")
print(positions_df.to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Serialize to JSON

# COMMAND ----------

# Convert DataFrame to list of dictionaries
records = positions_df.to_dict(orient="records")

# Serialize to JSON lines format (one object per line)
json_lines = "\n".join(json.dumps(r, default=str) for r in records)

print("✅ Serialized to JSON lines format")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Storage

# COMMAND ----------

# Get output path from config
output_path = config.positions_raw_path
file_name = f"{today}_positions.json"
full_path = f"{output_path}/{file_name}"

print(f"📂 Writing to: {full_path}")

# Write to Volume using dbutils
try:
    dbutils.fs.put(full_path, json_lines, overwrite=True)
    print(f"✅ Successfully wrote positions to {full_path}")
except Exception as e:
    print(f"❌ Error writing file: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Calculate summary statistics
summary = positions_df.groupby('direction').agg({
    'position_size': ['count', 'sum', 'mean']
}).round(2)

print("\n" + "="*60)
print("📊 POSITION GENERATION SUMMARY")
print("="*60)
print(f"Environment: {environment.upper()}")
print(f"Date: {today}")
print(f"Timezone: {config.pipeline_timezone}")
print(f"Desk: {config.desk_name}")
print(f"Total Positions: {len(positions_df)}")
print(f"\nPosition Breakdown:")
print(summary.to_string())
print("\n✅ Position generation completed successfully!")
print("="*60)
