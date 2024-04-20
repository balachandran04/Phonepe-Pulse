import git
import os
import pprint
import json
import pandas as pd
import pandas as pd
from sqlalchemy import create_engine
import pymysql

def connect():
    conn = pymysql.connect(host="localhost", user="root", passwd="admin@123", database="phonepe")
    return conn

# Aggregate datas

path="datas/data/aggregated/transaction/country/india/state/"
Agg_state_list=os.listdir(path)


agg_trans_data = {'State':[], 'Year':[],'Quater':[],'Transacion_type':[], 'Transacion_count':[], 'Transacion_amount':[]}
for i in Agg_state_list:
    state = path+i+"/"
    states = os.listdir(state)
    for k in states:
        year = state + k + "/"
        years = os.listdir(year)
        for j in years:
            josn = year + j
            data = open(josn,'r')
            D = json.load(data)
            for items  in D["data"]["transactionData"]:
                name = items["name"]
                count = items["paymentInstruments"][0]["count"]
                amount = items["paymentInstruments"][0]["amount"]

                agg_trans_data["State"].append(i)
                agg_trans_data["Year"].append(k) 
                agg_trans_data["Quater"].append(int(j.strip(".json")))
                agg_trans_data["Transacion_type"].append(name)
                agg_trans_data["Transacion_count"].append(count)
                agg_trans_data["Transacion_amount"].append(amount)



# ///////////////////////////////////////////////////////////////////////////////////////
paths="datas/data/aggregated/user/country/india/state/"
Agg_state_list=os.listdir(paths)        
agg_user = {'State':[], 'Year':[],'Quater':[],"brand":[],"count":[],"percentage":[]}


for i in Agg_state_list:
    state = paths+i+"/"
    states = os.listdir(state)
    for k in states:
        year = state + k + "/"
        years = os.listdir(year)
        for j in years:
            josn = year + j
            data = open(josn,'r')
            D = json.load(data)
            # pprint.pprint(D)
            try:
                for kk in D["data"]["usersByDevice"]:
                        brand = kk["brand"]
                        count = kk["count"]
                        percentage = kk["percentage"]
                        agg_user["State"].append(i)
                        agg_user["Year"].append(k) 
                        agg_user["Quater"].append(int(j.strip(".json")))
                        agg_user["brand"].append(brand)
                        agg_user["count"].append(count)
                        agg_user["percentage"].append(percentage)
                        
            except:
                 pass


# ///////////////////////////////////////////////////////
map_tran = {'State':[], 'Year':[],'Quater':[],"District":[],"Count":[],"Amount":[]}
path="datas/data/map/transaction/hover/country/india/state/"
map_state_list=os.listdir(path)
for i in map_state_list:
    state = path + i + "/"
    states = os.listdir(state)
    for k in states:
        year = state + k + "/"
        years = os.listdir(year)
        for j in years:
            jsons = year + j 
            data = open(jsons)
            d = json.load(data)
            
            for item in d["data"]["hoverDataList"]:
                district = item["name"]
                count = item["metric"][0]["count"]
                amount = item["metric"][0]["amount"]
                map_tran["State"].append(i)
                map_tran["Year"].append(k) 
                map_tran["Quater"].append(int(j.strip(".json")))
                map_tran["District"].append(district)
                map_tran["Count"].append(count)
                map_tran["Amount"].append(amount)


# //////////////////////////////////////////////////////////////////////
# map user
map_user = {'State':[], 'Year':[],'Quater':[],"District":[],"registeredUsers":[],"appOpens":[]}

path="datas/data/map/user/hover/country/india/state/"
map_state_list=os.listdir(path)
for i in map_state_list:
    state = path + i + "/"
    states = os.listdir(state)
    for k in states:
        year = state + k + "/"
        years = os.listdir(year)
        for  j in years:
            jsons = year + j 
            data = open(jsons,"r")
            d = json.load(data)
            for state_district, data in d["data"]["hoverData"].items():
                    register_user = data["registeredUsers"]
                    appopen = data["appOpens"]
                    destrict = state_district
                    map_user["State"].append(i)
                    map_user["Year"].append(k) 
                    map_user["Quater"].append(int(j.strip(".json")))
                    rg = map_user["registeredUsers"].append(register_user)
                    ao = map_user["appOpens"].append(appopen)
                    des = map_user["District"].append(destrict)



            
# /////////////////////////////////////////////
top_trans = {'State':[], 'Year':[],'Quater':[],"pincode":[],"count":[],"amount":[]}
path="datas/data/top/transaction/country/india/state/"
top_state_list=os.listdir(path)
for i in top_state_list:
    state = path + i + "/"
    states = os.listdir(state)
    for k in states:
        year = state + k + "/"
        years = os.listdir(year)
        for  j in years:
            jsons = year + j 
            data = open(jsons,"r")
            d = json.load(data)
            for  data in d["data"]["pincodes"]:
                    pincode = data["entityName"]
                    counts = data["metric"]["count"]
                    amounts = data["metric"]["amount"]
                    top_trans["State"].append(i)
                    top_trans["Year"].append(k) 
                    top_trans["Quater"].append(int(j.strip(".json")))
                    top_pin = top_trans["pincode"].append(pincode)
                    
                    top_count = top_trans["count"].append(counts)
                    top_amount = top_trans["amount"].append(amounts)

top_trans_f = pd.DataFrame(top_trans)

conn = connect()
cursor = conn.cursor()

insert = "insert into top_tran (State,Year,Quater,pincode,count,amount) values (%s,%s,%s,%s,%s,%s)"
values = top_trans_f.values.tolist()

# cursor.executemany(insert,values)
# conn.commit()

top_user = {'State':[], 'Year':[],'Quater':[],"pincode":[],"resgister_user":[]}
path="datas/data/top/user/country/india/state/"
top_user_list = os.listdir(path)
for i in top_user_list:
    state = path + i + "/"
    states = os.listdir(state)
    for k in states:
        year = state + k + "/"
        years = os.listdir(year)
        for  j in years:
            jsons = year + j 
            data = open(jsons,"r")
            d = json.load(data)
            for m in d["data"]["pincodes"]:
                    name = m["name"]
                    res_user = m["registeredUsers"]
                    top_user['State'].append(i)
                    top_user["Year"].append(k)
                    top_user['Quater'].append(int(j.strip(".json")))
                    pin = top_user["pincode"].append(name)
                    user = top_user["resgister_user"].append(res_user)





agg_trans_f= pd.DataFrame(agg_trans_data)
agg_user_f = pd.DataFrame(agg_user)
map_tran_f = pd.DataFrame(map_tran)
map_user_f = pd.DataFrame(map_user)  
top_trans_f = pd.DataFrame(top_trans)
top_user_f = pd.DataFrame(top_user)

top_user_f["State"] = top_user_f["State"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
top_user_f["State"] = top_user_f["State"].str.replace("-"," ")
top_user_f["State"] = top_user_f["State"].str.title()
top_user_f["State"] = top_user_f['State'].str.replace("dadra-&-nagar-haveli-&-daman-&-diu","Dadra and Nagar Haveli and Daman and Diu")

conn = connect()
cursor = conn.cursor()
query = "INSERT INTO agg_tran (state, years, quater, Transacion_type, Transacion_coun, Transacion_amount) VALUES (%s, %s, %s, %s, %s, %s)"
data = agg_trans_f.values.tolist()
# cursor.executemany(query, data)
# conn.commit()


conn = connect()
cursor = conn.cursor()
insert_data = "insert into agg_user (state,year,quater,brand,count,pencentage) values (%s,%s,%s,%s,%s,%s)"
data = agg_user_f.values.tolist()
# cursor.executemany(insert_data,data)
# conn.commit()


conn = connect()
cursor = conn.cursor()
insert_data =  "insert into map_tran (State,Year,Quater,District,Count,Amount) values (%s,%s,%s,%s,%s,%s)"
values = map_tran_f.values.tolist()
# cursor.executemany(insert_data,values)
# conn.commit()

                  
conn = connect()
cursor = conn.cursor()
insert_data = "insert into map_user (State,Year,Quater,District,register_user,appopen) values (%s,%s,%s,%s,%s,%s)"
values = map_user_f.values.tolist()
# cursor.executemany(insert_data,values)
# conn.commit()





conn = connect()
cursor = conn.cursor()
insert = "insert into top_tran (State,Year,Quater,pincode,count,amount) values (%s,%s,%s,%s,%s,%s)"
values = top_trans_f.values.tolist()
# cursor.executemany(insert,values)
# conn.commit()

conn = connect()
cursor = conn.cursor()
insert_datas = "insert into top_user (State,Year,Quater,pincode,Register_user) values (%s,%s,%s,%s,%s)"
values = top_user_f.values.tolist()
# cursor.executemany(insert_datas,values)
# conn.commit()


