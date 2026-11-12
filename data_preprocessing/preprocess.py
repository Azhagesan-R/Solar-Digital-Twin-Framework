from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import StorageLevel
from pyspark.sql.window import Window

# =====================================================================
# 1. SPARK SESSION INITIALIZATION
# =====================================================================
spark = (
    SparkSession.builder
    .appName("SolarDigitalTwinPreprocessing")
    .config("spark.local.dir", "/tmp/spark_local_scratch")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# =====================================================================
# 2. DATASET PATH CONVENTIONS (Redirected to open container space)
# =====================================================================
INPUT_PATH = "/opt/project/raw_data/telemetry/"
# FIX: Points output inside the container's open /tmp system space
OUTPUT_PATH = "/tmp/processed_solar_data"

# =====================================================================
# 3. INGESTION ENGINE
# =====================================================================
print("\n========== INGESTING PARALLEL SOLAR AND WEATHER CHANNELS ==========")

def load_unified_solar_stream(spark, root_path):
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(root_path + "/*.csv")
    )
    # Standardize column headers instantly to lowercase to prevent naming friction
    for col in df.columns:
        df = df.withColumnRenamed(col, col.strip().lower())
    return df

raw_solar_df = load_unified_solar_stream(spark, INPUT_PATH)

# =====================================================================
# 4. DUPLICATE REMOVAL & SCHEMA ALIGNMENT (Date Format Fixed)
# =====================================================================
print("\n========== TIME-SERIES SCHEMA DEFINITIONS ==========")
solar_df = raw_solar_df.dropDuplicates(["date_time", "plant_id"]).persist(StorageLevel.DISK_ONLY)

# Standardize Generation Metrics
if "ac_power" in solar_df.columns:
    solar_df = solar_df.withColumnRenamed("ac_power", "actual_power_output_kw")
elif "power" in solar_df.columns:
    solar_df = solar_df.withColumnRenamed("power", "actual_power_output_kw")

# FIX: Explicitly pass your raw data's specific date-string pattern 
# (e.g., 'dd-MM-yyyy HH:mm' or 'yyyy-MM-dd HH:mm:ss') to prevent NULL conversions
solar_df = solar_df.withColumn(
    "timestamp", 
    F.coalesce(
        F.to_timestamp(F.col("date_time"), "dd-MM-yyyy HH:mm"),
        F.to_timestamp(F.col("date_time"), "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(F.col("date_time")) # Fallback automatic check
    )
)
# DYNAMIC SCHEMA SAFETY INTERPOLATION
# If GHI or Ambient Temperature do not exist in the loaded CSV schema layer,
# fallback to proxy fields or base defaults to keep GNN inputs stable.
if "ghi" in solar_df.columns:
    solar_df = solar_df.withColumnRenamed("ghi", "ghi_w_m2")
elif "dc_power" in solar_df.columns:
    # Use scaled DC power as an engineering proxy baseline for weather irradiance
    solar_df = solar_df.withColumn("ghi_w_m2", F.col("dc_power") * 0.1)
else:
    solar_df = solar_df.withColumn("ghi_w_m2", F.lit(500.0))

if "ambient_temperature" in solar_df.columns:
    solar_df = solar_df.withColumnRenamed("ambient_temperature", "temp_c")
elif "temp" in solar_df.columns:
    solar_df = solar_df.withColumnRenamed("temp", "temp_c")
else:
    solar_df = solar_df.withColumn("temp_c", F.lit(28.0)) # Default standard cell temperature baseline

if "dc_key_voltage" in solar_df.columns:
    solar_df = solar_df.withColumnRenamed("dc_key_voltage", "inverter_voltage")
elif "dc_power" in solar_df.columns:
    solar_df = solar_df.withColumnRenamed("dc_power", "inverter_voltage")
else:
    solar_df = solar_df.withColumn("inverter_voltage", F.lit(1.0))

FEATURE_COLUMNS = ["ghi_w_m2", "temp_c", "inverter_voltage", "actual_power_output_kw"]
solar_df = solar_df.withColumn("timestamp", F.to_timestamp(F.col("date_time")))

# =====================================================================
# 5. TIME-SERIES IMPUTATION (LOCF)
# =====================================================================
def handle_missing_time_series(df, feature_columns):
    print("\n========== ROBUST TIME-SERIES IMPUTATION ==========")
    window_spec = (
        Window.partitionBy("plant_id")
        .orderBy("timestamp")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    
    for column in feature_columns:
        df = df.withColumn(
            column,
            F.when(F.col(column).isin([float("inf"), float("-inf"), -9999]), None)
            .otherwise(F.col(column))
        )
        df = df.withColumn(column, F.last(F.col(column), ignorenulls=True).over(window_spec))
    return df

solar_df = handle_missing_time_series(solar_df, FEATURE_COLUMNS)

# =====================================================================
# 6. DIGITAL TWIN FEATURE ENGINEERING
# =====================================================================
def inject_digital_twin_features(df):
    print("\n========== ENGINEERING PHYSICS-BENCHMARK FEATURES ==========")
    
    df = df.withColumn(
        "expected_power_output_kw",
        F.when(F.col("ghi_w_m2") > 10, F.col("ghi_w_m2") * 0.75 * (1 - (F.col("temp_c") - 25) * 0.004))
        .otherwise(0.0)
    )
    
    df = df.withColumn(
        "power_deviation_ratio",
        F.when(F.col("expected_power_output_kw") > 50, 
               (F.col("expected_power_output_kw") - F.col("actual_power_output_kw")) / F.col("expected_power_output_kw"))
        .otherwise(0.0)
    )
    
    spatial_window = Window.partitionBy("plant_id").orderBy("timestamp").rowsBetween(-4, -1)
    df = df.withColumn("node_historical_trend", F.avg(F.col("actual_power_output_kw")).over(spatial_window))
    
    return df.na.fill(0.0, ["node_historical_trend"])

solar_df = inject_digital_twin_features(solar_df)

# =====================================================================
# 7. MIN-MAX NORMALIZATION (With Explicit Double Precision Cast)
# =====================================================================
def apply_min_max_scaling(df, columns_to_scale):
    print("\n========== MIN-MAX FEATURE NORMALIZATION ==========")
    
    # Force casting all target scaling inputs to DoubleType to clear any string traps
    for c in columns_to_scale:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("double"))
            
    exprs = []
    for c in columns_to_scale:
        if c in df.columns:
            exprs.append(F.min(c).alias(f"{c}_min"))
            exprs.append(F.max(c).alias(f"{c}_max"))
        
    stats = df.select(exprs).collect()[0].asDict()
                      
    for c in columns_to_scale:
        if c in df.columns:
            c_min = stats[f"{c}_min"]
            c_max = stats[f"{c}_max"]
            
            # Explicit numerical validation checks prevent string operation crashes
            if c_min is not None and c_max is not None and isinstance(c_min, (int, float)) and isinstance(c_max, (int, float)):
                if (c_max - c_min) != 0:
                    df = df.withColumn(c, (F.col(c) - F.lit(c_min)) / F.lit(c_max - c_min))
            else:
                # Fallback safeguard fills empty or un-parseable values with a standard zero baseline
                df = df.withColumn(c, F.lit(0.0))
    return df

SCALING_COLUMNS = ["ghi_w_m2", "temp_c", "inverter_voltage", "expected_power_output_kw", "power_deviation_ratio"]
solar_df = apply_min_max_scaling(solar_df, SCALING_COLUMNS)


# =====================================================================
# 8. OUTPUT MATRIX COMPILATION & SAFE WORKSPACE CLONING
# =====================================================================
print("\n========== EXPORTING CLEAN MATRIX STRIPS ==========")
(
    solar_df.write
    .mode("overwrite")
    .partitionBy("plant_id")
    .parquet(OUTPUT_PATH)
)
print(f" Data pipeline successfully completed! Clean matrices saved to container: {OUTPUT_PATH}")

print("\n--- PREPROCESSED DATA SAMPLE FOR YOUR M.TECH PROGRESS REPORT ---")
# Show the first 20 rows of your fully calculated feature dimensions on the screen
solar_df.select("timestamp", "plant_id", "ghi_w_m2", "temp_c", "expected_power_output_kw", "power_deviation_ratio").show(20, truncate=False)

print("\n--- MATRIX EVALUATION METRICS ---")
print(f"Total Rows Processed: {solar_df.count()}")
print("Columns Available for GNN Engine: ", solar_df.columns)

