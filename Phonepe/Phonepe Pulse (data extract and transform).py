#importing libraries
from git.repo.base import Repo
import mysql.connector as sql
import streamlit as st
import os
import json
from streamlit_option_menu import option_menu
import pandas as pd
import plotly.express as px
from PIL import Image

#cloning git repo
Repo.clone_from("https://github.com/PhonePe/pulse.git","D:\Coding\Guvi-Assignments\Assignments-\Phonepe\git cloned")


p1 = "D:/Coding/Guvi-Assignments/Assignments-/Phonepe/git cloned/data/aggregated/transaction/country/india/state/"
agg_trans = os.listdir(p1)

c1 = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_type': [], 'Transaction_count': [],
            'Transaction_amount': []}
for state in agg_trans:
    cur_state = p1 + state + "/"
    agg_year = os.listdir(cur_state)
    
    for year in agg_year:
        cur_year = cur_state + year + "/"
        agg_file = os.listdir(cur_year)
        
        for file in agg_file:
            cur_file = cur_year + file
            data = open(cur_file, 'r')
            A = json.load(data)
            
            for i in A['data']['transactionData']:
                name = i['name']
                count = i['paymentInstruments'][0]['count']
                amount = i['paymentInstruments'][0]['amount']
                c1['Transaction_type'].append(name)
                c1['Transaction_count'].append(count)
                c1['Transaction_amount'].append(amount)
                c1['State'].append(state)
                c1['Year'].append(year)
                c1['Quarter'].append(int(file.strip('.json')))
                
df_agg_trans = pd.DataFrame(c1)



p2 = "D:/Coding/Guvi-Assignments/Assignments-/Phonepe/git cloned/data/aggregated/user/country/india/state/"
agg_user = os.listdir(p2)

c2 = {'State': [], 'Year': [], 'Quarter': [], 'Brands': [], 'Count': [],
            'Percentage': []}
for state in agg_user:
    cur_state = p2 + state + "/"
    agg_year = os.listdir(cur_state)
    
    for year in agg_year:
        cur_year = cur_state + year + "/"
        agg_file = os.listdir(cur_year)

        for file in agg_file:
            cur_file = cur_year + file
            data = open(cur_file, 'r')
            B = json.load(data)
            try:
                for i in B["data"]["usersByDevice"]:
                    brand_name = i["brand"]
                    counts = i["count"]
                    percents = i["percentage"]
                    c2["Brands"].append(brand_name)
                    c2["Count"].append(counts)
                    c2["Percentage"].append(percents)
                    c2["State"].append(state)
                    c2["Year"].append(year)
                    c2["Quarter"].append(int(file.strip('.json')))
            except:
                pass
df_agg_user = pd.DataFrame(c2)



p3 = "D:/Coding/Guvi-Assignments/Assignments-/Phonepe/git cloned/data/map/transaction/hover/country/india/state/"
map_trans = os.listdir(p3)

c3 = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'Count': [],
            'Amount': []}

for state in map_trans:
    cur_state = p3 + state + "/"
    map_year = os.listdir(cur_state)
    
    for year in map_year:
        cur_year = cur_state + year + "/"
        map_file = os.listdir(cur_year)
        
        for file in map_file:
            cur_file = cur_year + file
            data = open(cur_file, 'r')
            C = json.load(data)
            
            for i in C["data"]["hoverDataList"]:
                district = i["name"]
                count = i["metric"][0]["count"]
                amount = i["metric"][0]["amount"]
                c3["District"].append(district)
                c3["Count"].append(count)
                c3["Amount"].append(amount)
                c3['State'].append(state)
                c3['Year'].append(year)
                c3['Quarter'].append(int(file.strip('.json')))
                
df_map_trans = pd.DataFrame(c3)



p4 = "D:/Coding/Guvi-Assignments/Assignments-/Phonepe/git cloned/data/map/user/hover/country/india/state/"
map_user = os.listdir(p4)

c4 = {"State": [], "Year": [], "Quarter": [], "District": [],
            "RegisteredUser": [], "AppOpens": []}

for state in map_user:
    cur_state = p4 + state + "/"
    map_year = os.listdir(cur_state)
    
    for year in map_year:
        cur_year = cur_state + year + "/"
        map_file = os.listdir(cur_year)
        
        for file in map_file:
            cur_file = cur_year + file
            data = open(cur_file, 'r')
            D = json.load(data)
            
            for i in D["data"]["hoverData"].items():
                district = i[0]
                registeredUser = i[1]["registeredUsers"]
                appOpens = i[1]['appOpens']
                c4["District"].append(district)
                c4["RegisteredUser"].append(registeredUser)
                c4["AppOpens"].append(appOpens)
                c4['State'].append(state)
                c4['Year'].append(year)
                c4['Quarter'].append(int(file.strip('.json')))
                
df_map_user = pd.DataFrame(c4)



p5 = "D:/Coding/Guvi-Assignments/Assignments-/Phonepe//git cloned/data/top/transaction/country/india/state/"
top_trans = os.listdir(p5)

c5 = {'State': [], 'Year': [], 'Quarter': [], 'Pincode': [], 'Transaction_count': [],
            'Transaction_amount': []}

for state in top_trans:
    cur_state = p5 + state + "/"
    top_year = os.listdir(cur_state)
    
    for year in top_year:
        cur_year = cur_state + year + "/"
        top_file = os.listdir(cur_year)
        
        for file in top_file:
            cur_file = cur_year + file
            data = open(cur_file, 'r')
            E = json.load(data)
            
            for i in E['data']['pincodes']:
                name = i['entityName']
                count = i['metric']['count']
                amount = i['metric']['amount']
                c5['Pincode'].append(name)
                c5['Transaction_count'].append(count)
                c5['Transaction_amount'].append(amount)
                c5['State'].append(state)
                c5['Year'].append(year)
                c5['Quarter'].append(int(file.strip('.json')))
df_top_trans = pd.DataFrame(c5)



p6 = "D:/Coding/Guvi-Assignments/Assignments-/Phonepe/git cloned/data/top/user/country/india/state/"
top_user = os.listdir(p6)
c6 = {'State': [], 'Year': [], 'Quarter': [], 'Pincode': [],
            'RegisteredUsers': []}

for state in top_user:
    cur_state = p6 + state + "/"
    top_year = os.listdir(cur_state)
    
    for year in top_year:
        cur_year = cur_state + year + "/"
        top_file = os.listdir(cur_year)
        
        for file in top_file:
            cur_file = cur_year + file
            data = open(cur_file, 'r')
            F = json.load(data)
            
            for i in F['data']['pincodes']:
                name = i['name']
                registeredUsers = i['registeredUsers']
                c6['Pincode'].append(name)
                c6['RegisteredUsers'].append(registeredUsers)
                c6['State'].append(state)
                c6['Year'].append(year)
                c6['Quarter'].append(int(file.strip('.json')))
df_top_user = pd.DataFrame(c6)         


df_agg_trans.to_csv(r"D:\Coding\Guvi-Assignments\Assignments-\Phonepe\Data\agg_trans.csv",index=False)
df_agg_user.to_csv(r"D:\Coding\Guvi-Assignments\Assignments-\Phonepe\Data\agg_user.csv",index=False)
df_map_trans.to_csv(r"D:\Coding\Guvi-Assignments\Assignments-\Phonepe\Data\map_trans.csv",index=False)
df_map_user.to_csv(r"D:\Coding\Guvi-Assignments\Assignments-\Phonepe\Data\map_user.csv",index=False)
df_top_trans.to_csv(r"D:\Coding\Guvi-Assignments\Assignments-\Phonepe\Data\top_trans.csv",index=False)
df_top_user.to_csv(r"D:\Coding\Guvi-Assignments\Assignments-\Phonepe\Data\top_user.csv",index=False)


# CONNECTING WITH MYSQL DATABASE
mydb = sql.connect(host="localhost",
                   user="root",
                   password="",
                  )
mycursor = mydb.cursor(buffered=True)

#Checking if database exists if not make 
mycursor.execute("CREATE DATABASE IF NOT EXISTS phonepe_pulse")
mycursor.execute("USE phonepe_pulse")

#CREATE TABLE IN SQL IF NOT EXISTS
mycursor.execute("create table if not exists agg_trans (State varchar(255), Year int, Quarter int, Transaction_type varchar(255), Transaction_count int, Transaction_amount float)")

for i,row in df_agg_trans.iterrows():
    sql = "INSERT INTO agg_trans VALUES (%s,%s,%s,%s,%s,%s)"
    mycursor.execute(sql, tuple(row))
    mydb.commit()

mycursor.execute("create table if not exists agg_user (State varchar(255), Year int, Quarter int, Brands varchar(255), Count int, Percentage float)")

for i,row in df_agg_user.iterrows():
    sql = "INSERT INTO agg_user VALUES (%s,%s,%s,%s,%s,%s)"
    mycursor.execute(sql, tuple(row))
    mydb.commit()

mycursor.execute("create table if not exists map_trans (State varchar(255), Year int, Quarter int, District varchar(255), Count bigint, Amount float)")

for i,row in df_map_trans.iterrows():
    sql = "INSERT INTO map_trans VALUES (%s,%s,%s,%s,%s,%s)"
    mycursor.execute(sql, tuple(row))
    mydb.commit()

mycursor.execute("create table if not exists map_user (State varchar(255), Year int, Quarter int, District varchar(255), Registered_user bigint, App_opens bigint)")

for i,row in df_map_user.iterrows():
    sql = "INSERT INTO map_user VALUES (%s,%s,%s,%s,%s,%s)"
    mycursor.execute(sql, tuple(row))
    mydb.commit()

mycursor.execute("create table if not exists top_trans (State varchar(255), Year int, Quarter int, Pincode int, Transaction_count bigint, Transaction_amount float)")

for i,row in df_top_trans.iterrows():
    sql = "INSERT INTO top_trans VALUES (%s,%s,%s,%s,%s,%s)"
    mycursor.execute(sql, tuple(row))
    mydb.commit()

mycursor.execute("create table if not exists top_user (State varchar(255), Year int, Quarter int, Pincode int, Registered_users bigint)")

for i,row in df_top_user.iterrows():
    sql = "INSERT INTO top_user VALUES (%s,%s,%s,%s,%s)"
    mycursor.execute(sql, tuple(row))
    mydb.commit()



