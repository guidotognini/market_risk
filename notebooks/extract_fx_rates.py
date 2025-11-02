# Databricks notebook source
# MAGIC %md
# MAGIC # Daily FX Rates Extraction
# MAGIC
# MAGIC Extracts daily FX rates from Polygon.io API for configured currency pairs.
# MAGIC
# MAGIC **Configuration-driven:** All parameters loaded from YAML config files.
# MAGIC
# MAGIC **Currency Pairs:**
# MAGIC - Configured in `config/pipeline_config.yaml`
# MAGIC - Environment-specific paths in `config/{environment}.yaml`

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
import requests
from datetime import datetime
import pytz
from pyspark.sql import SparkSession

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Configuration

# COMMAND ----------

# Create widget for environment selection
dbutils.widgets.dropdown("environment", "prod", ["dev", "prod"], "Environment")

# Load configuration
environment = dbutils.widgets.get("environment")
config = load_config(environment=environment)

print(f"🚀 FX Rates Extraction - {environment.upper()} Environment")
print(f"📊 Catalog: {config.catalog}")
print(f"📂 Schema: {config.schema}")
print(f"💱 Currency Pairs: {', '.join(config.get_currency_symbols())}")
print(f"🕐 Timezone: {config.pipeline_timezone}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Spark Session

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Determine Date Range

# COMMAND ----------

# Get current date in configured timezone
tz = pytz.timezone(config.pipeline_timezone)
current_date = datetime.now(tz).strftime("%Y-%m-%d")

# Query maximum date from silver table to determine FROM_DATE
silver_fx_table = config.get_table_name('silver', 'fx_rates')

try:
    from_date = spark.sql(f"SELECT MAX(date) FROM {silver_fx_table}").collect()[0][0]
    if from_date is None:
        # If table is empty, start from configured minimum date
        from_date = config.minimum_data_date
        print(f"⚠️  No existing data found. Starting from: {from_date}")
    else:
        print(f"📅 Last processed date: {from_date}")
except Exception as e:
    # If table doesn't exist, start from configured minimum date
    from_date = config.minimum_data_date
    print(f"⚠️  Silver table not found. Starting from: {from_date}")

to_date = current_date

print(f"📥 Fetching data from {from_date} to {to_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get API Credentials

# COMMAND ----------

# Get Polygon API key from Databricks secrets
api_key = dbutils.secrets.get(
    scope=config.polygon_secret_scope,
    key=config.polygon_secret_key
)

print("✅ API credentials loaded from secrets")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract FX Rates from Polygon API

# COMMAND ----------

# Setup output directory from config
output_dir = Path(config.fx_rates_raw_path)

# Get currency pairs from config
currency_pairs = config.get_currency_symbols()

print(f"📂 Output directory: {output_dir}")
print(f"💱 Processing {len(currency_pairs)} currency pairs...")

# Extract data for each currency pair
successful_downloads = 0
failed_downloads = []

for pair in currency_pairs:
    try:
        # Build API URL using Polygon endpoint
        url = f"{config.polygon_base_url}/v2/aggs/ticker/C:{pair}/range/1/day/{from_date}/{to_date}?apiKey={api_key}"

        # Make API request with timeout from config
        r = requests.get(url, timeout=30)
        r.raise_for_status()

        # Write response to file
        output_file = output_dir / f"{pair}_{from_date}_{to_date}.json"
        output_file.write_text(r.text)

        successful_downloads += 1
        print(f"✅ Downloaded {pair}")

    except requests.exceptions.RequestException as e:
        failed_downloads.append(pair)
        print(f"❌ Failed to download {pair}: {str(e)}")

    except Exception as e:
        failed_downloads.append(pair)
        print(f"❌ Error processing {pair}: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "="*60)
print("📊 EXTRACTION SUMMARY")
print("="*60)
print(f"Environment: {environment.upper()}")
print(f"Date Range: {from_date} to {to_date}")
print(f"Total Pairs: {len(currency_pairs)}")
print(f"✅ Successful: {successful_downloads}")
print(f"❌ Failed: {len(failed_downloads)}")

if failed_downloads:
    print(f"\nFailed pairs: {', '.join(failed_downloads)}")
    raise Exception(f"Failed to download {len(failed_downloads)} currency pairs")

print("\n✅ Extraction completed successfully!")
print("="*60)
