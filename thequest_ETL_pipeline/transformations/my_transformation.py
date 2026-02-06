# Databricks notebook source
import dlt
from pyspark.sql.functions import *

# Bronze: Raw JSON from Volume
@dlt.table(
  name="bronze_population",
  comment="Raw population data from Data USA API stored in volume",
  table_properties={
    "quality": "bronze",
    "delta.columnMapping.mode": "name" 
  }
)

def bronze_population():
  
  # Get the most recent JSON file
  files = dbutils.fs.ls("/Volumes/thequest/main/datausa_io")
  latest_file = sorted([f for f in files if f.name.endswith('.json')], 
                       key=lambda x: x.name, reverse=True)[0]
  
  # Read the entire JSON structure
  return (spark.read
    .option("multiLine", "true")
    .json(latest_file.path)
    .withColumn("_source_file", lit(latest_file.name))
    .withColumn("_ingestion_timestamp", current_timestamp())
  )

# Silver: Population data 
@dlt.table(
  name="silver_population",
  comment="Cleaned US population data by year - all string fields trimmed",
  table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_year", "year IS NOT NULL")
@dlt.expect_or_drop("valid_population", "population > 0")
def silver_population():
  
  return (dlt.read("bronze_population")
    .select(
      explode(col("data.data")).alias("record"),
      col("_source_file"),
      col("_ingestion_timestamp")
    )
    .select(
      trim(col("record.`Nation ID`")).alias("nation_id"),
      trim(col("record.Nation")).alias("nation"),
      col("record.Year").cast("int").alias("year"),
      col("record.Population").cast("long").alias("population"),
      trim(col("_source_file")).alias("_source_file"),
      col("_ingestion_timestamp")
    )
  )



  # Silver: BLS data (reads from bronze table created by Auto Loader)
@dlt.table(
  name="silver_bls_pr_data_current",
  comment="Cleaned BLS productivity data with trimmed columns and proper types",
  table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_year", "year IS NOT NULL")
@dlt.expect_or_drop("valid_value", "value IS NOT NULL")
def silver_bls_pr_data_current():
  
  # Read from the bronze table created by the Auto Loader notebook
  return (dlt.read_stream("thequest.main.bronze_bls_pr_data_current")
    .select(
      trim(col("series_id")).alias("series_id"),
      trim(col("year")).cast("smallint").alias("year"),
      trim(col("period")).alias("period"),
      trim(col("value")).cast("double").alias("value"),
      trim(col("footnote_codes")).alias("footnote_codes"),
      col("_source_file"),
      col("_ingestion_time")
    )
  )

#################
##### Gold  #####
#################

# Gold: Population Mean and Standard Deviation (2013-2018)
@dlt.table(
  name="gold_population_mean_stddev",
  comment="Mean and standard deviation of US population for years 2013-2018",
  table_properties={"quality": "gold"}
)
def gold_population_mean_stddev():
  
  return (dlt.read("silver_population")
    .filter((col("year") >= 2013) & (col("year") <= 2018))
    .select(
      mean("population").alias("mean_population"),
      stddev("population").alias("stddev_population"),
      min("year").alias("start_year"),
      max("year").alias("end_year"),
      count("*").alias("year_count")
    )
  )

# Gold: Best Year per Series ID
@dlt.table(
  name="gold_best_year",
  comment="Best year (highest quarterly sum) for each BLS series_id",
  table_properties={"quality": "gold"}
)
def gold_best_year():
  
  from pyspark.sql.window import Window
  
  # Sum quarterly values by series and year
  yearly_sums = (dlt.read("silver_bls_pr_data_current")
    .filter(col("period").like("Q%"))
    .groupBy("series_id", "year")
    .agg(sum("value").alias("total_value"))
  )
  
  # Find max value per series
  window_spec = Window.partitionBy("series_id")
  
  return (yearly_sums
    .withColumn("max_value", max("total_value").over(window_spec))
    .filter(col("total_value") == col("max_value"))
    .select("series_id", "year", col("total_value").alias("value"))
    .orderBy("series_id", "year")
  )

# Gold: BLS Series with Population Data
@dlt.table(
  name="gold_population_series_filtered",
  comment="BLS series PRS30006032 Q01 joined with population data",
  table_properties={"quality": "gold"}
)
def gold_population_series_filtered():
  
  # Filter BLS data
  bls_filtered = (dlt.read("silver_bls_pr_data_current")
    .filter((col("series_id") == "PRS30006032") & (col("period") == "Q01"))
    .select("series_id", "year", "period", "value")
  )
  
  # Get population data
  population = (dlt.read("silver_population")
    .select(col("year").alias("pop_year"), "population")
  )
  
  # Join
  return (bls_filtered
    .join(population, bls_filtered.year == population.pop_year, "left")
    .select("series_id", "year", "period", "value", "population")
    .orderBy("year")
  )