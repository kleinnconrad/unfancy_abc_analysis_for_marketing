import pandas as pd
import numpy as np
import json

def apply_predictions(data_path, thresholds_path, output_path):
    print("Task: Applying Predictive Segments to New Cohort...")
    
    df = pd.read_csv(data_path)
    
    with open(thresholds_path, 'r') as f:
        thresholds = json.load(f)
        
    c_thresh = thresholds['content_threshold']
    l_thresh = thresholds['login_threshold']
    
    df['predictive_segment'] = np.where(
        (df['first_14_days_content_consumed'] >= c_thresh) & 
        (df['first_14_days_login_days'] >= l_thresh),
        'High-Potential VIP (Trigger Premium Upsell)',
        'Standard (Trigger General Nurture)'
    )
    
    df.to_csv(output_path, index=False)
    print(df[['customer_id', 'predictive_segment']].head(10))
    print(f"\nPredictions saved to {output_path}")

if __name__ == "__main__":
    apply_predictions(
        data_path='data/raw_new_cohort.csv',
        thresholds_path='data/magic_thresholds.json',
        output_path='data/predicted_new_cohort.csv'
    )
