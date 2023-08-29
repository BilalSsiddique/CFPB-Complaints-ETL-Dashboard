import MySQLdb
import pandas as pd
import pygsheets



# Define your MySQL connection parameters
mysql_host = 'localhost'
mysql_user = 'root'
mysql_password = 'perfect'
mysql_db = 'mysql'


def connection():
    conn = MySQLdb.connect(host=mysql_host, user=mysql_user, passwd=mysql_password, db=mysql_db)
    return conn

def create_table(conn):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS Complaints (
        product VARCHAR(255),
        complaint_what_happened TEXT,
        date_sent_to_company DATETIME,
        issue VARCHAR(255),
        sub_product VARCHAR(255),
        zip_code VARCHAR(10),
        tags TEXT,
        has_narrative BOOLEAN,
        complaint_id VARCHAR(255) PRIMARY KEY,
        timely VARCHAR(10),
        consumer_consent_provided TEXT,
        company_response VARCHAR(255),
        submitted_via VARCHAR(255),
        company VARCHAR(255),
        date_received DATETIME,
        state VARCHAR(2),
        consumer_disputed VARCHAR(10),
        company_public_response TEXT,
        sub_issue VARCHAR(255),
        sort FLOAT
    );
    """
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_table_sql)
        
        conn.commit()
    finally:
        print('successfull')


def insert_data_into_database(data_list):
    conn = connection()
    create_table(conn)

    try:
        with conn.cursor() as cursor:
            for data in data_list:
                columns = ', '.join(data['_source'].keys())
                placeholders = ', '.join(['%s'] * len(data['_source']))
                insert_query = f"INSERT INTO Complaints ({columns}) VALUES ({placeholders})"
                values = tuple(data['_source'].values())
                cursor.execute(insert_query, values)
        
        conn.commit()
    finally:
        conn.close()
        return 'successfully dumped the data' 
    



def write_to_gsheet(spreadsheet_id, sheet_name, data_df,service_file_path='./scripts/cfbp-etl-2fea04d28428.json'):
    gc = pygsheets.authorize(service_file=service_file_path)
    sh = gc.open_by_key(spreadsheet_id)
    try:
        sh.add_worksheet(sheet_name)
    except:
        pass
    wks_write = sh.worksheet_by_title(sheet_name)
    wks_write.clear('A1',None,'*')
    wks_write.set_dataframe(data_df, (1,1), encoding='utf-8', fit=True)
    wks_write.frozen_rows = 1
    

        



    






# data= [
#     {'_index': 'complaint-public-v2', '_type': '_doc', '_id': '7448438', '_score': 1.0, '_source': {'product': 'Credit reporting, credit repair services, or other personal consumer reports', 'complaint_what_happened': '', 'date_sent_to_company': '2023-08-23T12:00:00-05:00', 'issue': 'Incorrect information on your report', 'sub_product': 'Credit reporting', 'zip_code': '357XX', 'tags': None, 'has_narrative': False, 'complaint_id': '7448438', 'timely': 'Yes', 'consumer_consent_provided': None, 'company_response': 'In progress', 'submitted_via': 'Web', 'company': 'EQUIFAX, INC.', 'date_received': '2023-08-23T12:00:00-05:00', 'state': 'AL', 'consumer_disputed': 'N/A', 'company_public_response': None, 'sub_issue': 'Information belongs to someone else'}, 'sort': [1.0, '7448438']}
# ]

# insert_data_into_database(data)
# extract_data_from_database()
# print(dt[1])
