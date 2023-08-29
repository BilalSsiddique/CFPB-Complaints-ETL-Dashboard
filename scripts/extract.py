from datetime import date, datetime, timedelta
import requests,os,sys

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from scripts.load import connection



class Request_Data():
    
    def __init__(self):
        self.response_data= []
        self.size=500
        self.states= None
        self.max_date= None
        self.min_date=None
        self.url='https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/?field=complaint_what_happened&size={}&date_received_max={}&date_received_min={}&state={}'

    def define_dates(self,time_delta=365,data_string = '30-Apr-22'):

        max_date = datetime.strptime(data_string,'%d-%b-%y')
        self.max_date = max_date.strftime("%Y-%m-%d")
        self.max_date  = datetime.strptime(self.max_date,"%Y-%m-%d").date()
        self.min_date = (self.max_date - timedelta(days=time_delta)).strftime("%Y-%m-%d")
        

    def request_states(self,states_url='https://gist.githubusercontent.com/mshafrir/2646763/raw/8b0dbb93521f5d6889502305335104218454c2bf/states_hash.json'):
        response = requests.get(states_url)
        if response.ok:
            self.states= list(response.json().keys())
        else:
            self.states=[]

    def make_request(self,url,state):
        resp = requests.get(url)
        if resp.ok:
            res_json = resp.json()
            records_cap= 0
            for data in res_json['hits']['hits']:
                
                if records_cap >= 500:
                    return 
                else:
                    self.response_data.append(data) 
                    records_cap+=1
                print('length',len(self.response_data),'request for state :',state)
        else:
            print(f'Error : {resp.status_code}')

    def prepare_url(self):
        if self.states:
            for state in self.states:
                new_url=self.url.format(self.size,self.max_date,self.min_date,state)
                self.make_request(new_url,state)
            print('done')
            return self.response_data
        else:
            return 'error: states not found / States List is Empty'
    
    def execute(self):
        self.define_dates()
        self.request_states()
        resp_data =self.prepare_url()
        if isinstance(resp_data,list) and resp_data:
            return resp_data
    


############################################ EXTRACTION FROM DB ################################


def extract_data_from_database():
    
    try:
        conn= connection()
        with conn.cursor() as cursor:
            print('check')
            select_query = f"select * from Complaints"
            cursor.execute(select_query)
            result = cursor.fetchall()
            if result:
                columns = [desc[0] for desc in cursor.description]
                print ('successfully Load the data')
                return [result,columns]
                
            else:
                return False
    
    except Exception:
        print('error orccured while loading data from database')
    
    finally:
        conn.close()
