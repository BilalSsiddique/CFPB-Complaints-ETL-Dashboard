from datetime import date, datetime, timedelta
import requests

class Request_Data():
    def __init__(self):
        self.response_data= []
        self.size=500
        self.states= None
        self.max_date= None
        self.min_date=None
        self.url='https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/?field=complaint_what_happened&size={}&date_received_max={}&date_received_min={}&state={}'

    def define_dates(self):
        time_delta=365
        max_date = (date.today()).strftime("%Y-%m-%d")
        min_date = (date.today() - timedelta(days=time_delta)).strftime("%Y-%m-%d")
        self.max_date= max_date
        self.min_date= min_date

    def generate_states(self):
        response = requests.get(self.STATES_URL)
        if response.ok:
            self.states= list(response.json().keys())
        return []

    def make_request(self,url,state):
        resp = requests.get(url)
        if resp.ok:
            res_json = resp.json()
            for data in res_json['hits']['hits']:
                self.response_data.append(data) 
            #     print(data['_source'])
            #     break
                print('length',len(self.response_data),'request for state :',state)
        else:
            print(f'Error : {resp.status_code}')
            return

    def prepare_url(self):
        if self.states:
            for state in self.states:
                # print(state)
                # print(self.url.format(self.size,self.max_date,self.min_date,state))
                new_url=self.url.format(self.size,self.max_date,self.min_date,state)
                self.make_request(new_url,state)
            print('done')
            return self.response_data
        else:
            return 'error: states not found / States List is Empty'


    


obj = Request_Data() #
obj.define_dates()
obj.generate_states()
obj.prepare_url()

# print(obj.response_data)
