import pandas as pd
import json

def extract_thresholds(input_path, output_path):
    print("Task: Extracting Diagnostic Thresholds...")
    df = pd.read_csv(input_path)
    
    profile = df.groupby('abc_class').agg(
        Avg_Early_Content_Views=('first_14_days_content_consumed', 'mean'),
        Avg_Early_Login_Days=('first_14_days_login_days', 'mean')
    )
    
    a_avg_content = profile.loc['A_VIP', 'Avg_Early_Content_Views']
    a_avg_logins = profile.loc['A_VIP', 'Avg_Early_Login_Days']
    
    # Calculate thresholds at 70% of the A-Class average
    thresholds = {
        'content_threshold': int(a_avg_content * 0.70),
        'login_threshold': int(a_avg_logins * 0.70)
    }
    
    with open(output_path, 'w') as f:
        json.dump(thresholds, f)
        
    print(f"Discovered Thresholds: {thresholds}")
    print(f"Output saved to {output_path}")

if __name__ == "__main__":
    extract_thresholds(
        input_path='/Volumes/workspace/default/data/labeled_historical_customers.csv',
        output_path='/Volumes/workspace/default/data/magic_thresholds.json'
    )
