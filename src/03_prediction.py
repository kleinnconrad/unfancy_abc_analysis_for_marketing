import pandas as pd
import numpy as np
import json
from pyspark.sql import SparkSession

def apply_predictions_and_save_delta(data_path, thresholds_path, table_name):
    print("Task: Applying Predictive Segments to New Cohort...")
    
    # Initialize Spark Session (automatically available in Databricks environments)
    spark = SparkSession.builder.getOrCreate()
    
    # 1. Load Data (Step 0 Outcome)
    df = pd.read_csv(data_path)
    
    # 2. Load Thresholds (Step 2 Outcome)
    with open(thresholds_path, 'r') as f:
        thresholds = json.load(f)
        
    c_thresh = thresholds['content_threshold']
    l_thresh = thresholds['login_threshold']
    
    # 3. Apply the Unfancified IF-THEN logic (Step 3 Outcome)
    df['predictive_segment'] = np.where(
        (df['first_14_days_content_consumed'] >= c_thresh) & 
        (df['first_14_days_login_days'] >= l_thresh),
        'High-Potential VIP (Trigger Premium Upsell)',
        'Standard (Trigger General Nurture)'
    )
    
    # 4. Enrich the DataFrame with the diagnostic metadata for auditability
    df['applied_content_threshold'] = c_thresh
    df['applied_login_threshold'] = l_thresh
    
    print(df[['customer_id', 'predictive_segment']].head(10))
    
    # 5. Convert Pandas DataFrame to Spark DataFrame
    spark_df = spark.createDataFrame(df)
    
    # 6. Write to Delta Table in the specified Databricks Catalog/Schema
    print(f"\nWriting comprehensive outcomes to Delta table: {table_name}")
    spark_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(table_name)
        
    print("Task Complete.")

if __name__ == "__main__":
    # Note: When running in Databricks via DABs, ensure these point to valid 
    # DBFS or Unity Catalog Volume paths (e.g., '/Volumes/workspace/default/data/...')
    apply_predictions_and_save_delta(
        data_path='/Volumes/workspace/default/data/raw_new_cohort.csv',
        thresholds_path='/Volumes/workspace/default/data/magic_thresholds.json',
        table_name='workspace.default.marketing_cohort_predictions'
    )
