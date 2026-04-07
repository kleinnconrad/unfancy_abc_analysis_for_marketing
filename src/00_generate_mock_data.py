import pandas as pd
import numpy as np
import os

def create_mockup_data(n_customers=1000):
    np.random.seed(42)
    revenues = np.random.lognormal(mean=3.5, sigma=1.2, size=n_customers)
    
    df = pd.DataFrame({
        'customer_id': range(1, n_customers + 1),
        '12m_spend_usd': revenues,
    })
    
    df['first_14_days_content_consumed'] = (df['12m_spend_usd'] * 0.4) + np.random.normal(5, 3, n_customers)
    df['first_14_days_login_days'] = (df['12m_spend_usd'] * 0.05) + np.random.normal(3, 2, n_customers)
    
    df['first_14_days_content_consumed'] = df['first_14_days_content_consumed'].clip(lower=0).round(0).astype(int)
    df['first_14_days_login_days'] = df['first_14_days_login_days'].clip(lower=0, upper=14).round(0).astype(int)
    
    return df

if __name__ == "__main__":
    print("Task: Generating raw mock data...")
    os.makedirs('data', exist_ok=True)
    
    # 1. Historical data for training/characterization
    historical_df = create_mockup_data(1000)
    historical_df.to_csv('data/raw_historical_customers.csv', index=False)
    
    # 2. New cohort data for prediction
    new_cohort_df = pd.DataFrame({
        'customer_id': range(5001, 5011),
        'first_14_days_content_consumed': [3, 45, 12, 60, 2, 38, 15, 0, 85, 8],
        'first_14_days_login_days': [1, 9, 4, 12, 1, 8, 3, 0, 14, 2]
    })
    new_cohort_df.to_csv('data/raw_new_cohort.csv', index=False)
    
    print("Output saved to data/raw_historical_customers.csv and data/raw_new_cohort.csv")
