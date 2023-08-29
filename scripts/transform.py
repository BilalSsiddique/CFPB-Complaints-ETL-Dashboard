import pandas as pd

def transform_data_pandas(db_data):
    
    columns = db_data[1]
    data= db_data[0]
    df = pd.DataFrame(data, columns=columns)
    columns_to_drop = [columns[1], columns[2],columns[5],columns[6],columns[7],columns[10],columns[16],columns[17]]
    print(columns_to_drop)
    df.drop(columns=columns_to_drop, inplace=True)

    print("DataFrame after dropping columns:")
    groupby_columns = [
    'product', 'issue', 'sub_product','timely',
    'company_response', 'submitted_via', 'company', 'date_received', 'state', 'sub_issue'
    ]

    grouped = df.groupby(groupby_columns).agg({'complaint_id': 'nunique'}).reset_index()
    grouped['month_year'] = pd.to_datetime(grouped['date_received']).dt.to_period('M').dt.to_timestamp('M')

    grouped.rename(columns={'complaint_id': 'distinct_complaint_count'}, inplace=True)
    return grouped
