import pandas as pd
import numpy as np

def run_abc_analysis(input_path, output_path):
    print("Task: Running ABC Segmentation...")
    df = pd.read_csv(input_path)
    
    df = df.sort_values('12m_spend_usd', ascending=False).copy()
    
    total_revenue = df['12m_spend_usd'].sum()
    df['cumulative_revenue'] = df['12m_spend_usd'].cumsum()
    df['cumulative_percentage'] = df['cumulative_revenue'] / total_revenue
    
    df['abc_class'] = np.where(df['cumulative_percentage'] <= 0.80, 'A_VIP',
                      np.where(df['cumulative_percentage'] <= 0.95, 'B_Core', 'C_LongTail'))
    
    df.to_csv(output_path, index=False)
    print(f"Output saved to {output_path}")

if __name__ == "__main__":
    run_abc_analysis(
        input_path='data/raw_historical_customers.csv', 
        output_path='data/labeled_historical_customers.csv'
    )
