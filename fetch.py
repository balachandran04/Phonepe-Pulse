import pymysql
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
import plotly.express as px
import requests
import json

def conncetion():
    conn = pymysql.connect(host="localhost",user="root",passwd="admin@123",database="phonepe")
    return conn

conn = conncetion()
cursor = conn.cursor()
query = "select * from agg_tran"
cursor.execute(query)
lose = cursor.fetchall()
data = pd.DataFrame(lose,columns=["State","Year","Quater","transaction_type","transation_count","transation_amount"])


cursor = conn.cursor()
query = "select * from agg_user"
cursor.execute(query)
result = cursor.fetchall()
agg_user = pd.DataFrame(result,columns=["State","Year","Quater","brand","transation_count","pencentage"])

cursor = conn.cursor()
query = "select * from map_tran"
cursor.execute(query)
result = cursor.fetchall()
map_tran = pd.DataFrame(result,columns=["State","Year","Quater","District","transation_count","transation_amount"])

cursor = conn.cursor()
query = "select * from map_user"
cursor.execute(query)
result = cursor.fetchall()
map_user = pd.DataFrame(result,columns=["State","Year","Quater","District","register_user","appopen"])

cursor = conn.cursor()
query = "select * from top_tran"
cursor.execute(query)
result = cursor.fetchall()
top_tran = pd.DataFrame(result,columns=["State","Year","Quater","pincode","transation_count","transation_amount"])

cursor = conn.cursor()
query = "select * from top_user"
cursor.execute(query)
result = cursor.fetchall()

top_user = pd.DataFrame(result,columns=["State","Year","Quater","pincode","register_user"])

# agg_transation year of count and amount
def tran_count_amount(df ,year):
    amounts = df[df["Year"]== year]
    amounts.reset_index(drop=True,inplace=True)

    amountsg =  amounts.groupby("State")[["transation_count","transation_amount"]].sum()
    amountsg.reset_index(inplace=True)
    fig_counts = px.bar(amountsg,x="State", y="transation_amount",title=f"{year} Transation amount",width=600,height=600)
    st.plotly_chart(fig_counts)
    
    fig_amounts = px.bar(amountsg,x="State", y="transation_count", title=f"{year} Transation count",height=600)
    st.plotly_chart(fig_amounts)

    st.subheader("Map")

    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)
    data1 = json.loads(response.content)
    state_name = []
    for i in data1["features"]:
        state_name.append(i["properties"]["ST_NM"])
    state_name.sort()

    fig_india = px.choropleth(amountsg, geojson=data1, locations="State", featureidkey="properties.ST_NM",
                              color="transation_amount",color_continuous_scale="Turbo",
                              range_color=(amountsg["transation_amount"].min(),amountsg["transation_amount"].max()),
                              hover_name="State",title=f"{year} transation_amount", fitbounds="locations",
                              width=600,height=600)
    fig_india.update_geos(visible=False) 
    st.plotly_chart(fig_india)
    fig_india_2 = px.choropleth(amountsg, geojson=data1, locations="State", featureidkey="properties.ST_NM",
                              color="transation_count",color_continuous_scale="Turbo",
                              range_color=(amountsg["transation_count"].min(),amountsg["transation_count"].max()),
                              hover_name="State",title=f"{year} transation_count", fitbounds="locations",
                              width=600,height=600)
    fig_india_2.update_geos(visible=False) 
    st.plotly_chart(fig_india_2)
    return amounts

def tran_count_amount_quater(df ,quater):
    amounts = df[df["Quater"]== quater]
    amounts.reset_index(drop=True,inplace=True)
    amountsg =  amounts.groupby("State")[["transation_count","transation_amount"]].sum()
    amountsg.reset_index(inplace=True)
    fig_count = px.bar(amountsg,x="State", y="transation_amount",title=f"{amounts['Year'].unique()} year  {quater} quater Transation amount",width=600,height=600)
    st.plotly_chart(fig_count)  
    fig_amount = px.bar(amountsg,x="State", y="transation_count", title=f"{amounts['Year'].unique()} year  {quater} quater Transation count",width=600,height=600)
     
    st.plotly_chart(fig_amount)

    col1,col2 = st.columns(2) 
    with col1:
        url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response = requests.get(url)
        data1 = json.loads(response.content)
        state_name = []
        for i in data1["features"]:
            state_name.append(i["properties"]["ST_NM"])
        state_name.sort()

        fig_india = px.choropleth(amountsg, geojson=data1, locations="State", featureidkey="properties.ST_NM",
                                color="transation_amount",color_continuous_scale="Turbo",
                                range_color=(amountsg["transation_amount"].min(),amountsg["transation_amount"].max()),
                                hover_name="State",title=f"{amounts['Year'].unique()} year {quater} quater transation_amount", fitbounds="locations",
                                width=600,height=600)
        fig_india.update_geos(visible=False) 
        st.plotly_chart(fig_india)
    with col2:   
        fig_india_2 = px.choropleth(amountsg, geojson=data1, locations="State", featureidkey="properties.ST_NM",
                                color="transation_count",color_continuous_scale="Turbo",
                                range_color=(amountsg["transation_count"].min(),amountsg["transation_count"].max()),
                                hover_name="State",title=f"{amounts['Year'].unique()} year {quater} quater transation_count", fitbounds="locations",
                                width=600,height=600)
        fig_india_2.update_geos(visible=False) 
        st.plotly_chart(fig_india_2)
        


def agg_tran_type(df,state):
    trans_type = df[df["State"] == state]
    trans_type.reset_index(drop=True,inplace=True)
    amountsg =  trans_type.groupby("transaction_type")[["transation_count","transation_amount"]].sum()
    amountsg.reset_index(inplace=True)
    col1,col2 = st.columns(2)
    with col1:
        fig_amounts = px.pie(amountsg,names="transaction_type",values="transation_amount",
                    color_discrete_sequence=px.colors.qualitative.Set3,hole=0.5,width=300,height=300,title=f"{state} of  transation amount ")
        st.plotly_chart(fig_amounts)
    with col2:    
        fig = px.pie(amountsg,names="transaction_type",values="transation_count",
                    color_discrete_sequence=px.colors.qualitative.Set3,hole=0.5,width=300,height=300,title=f"{state} of transation count")
        st.plotly_chart(fig)
        
# agg user analysis

def agg_user_tran_bran(df,year):
    agg_user_year = df[df["Year"] == year]
    agg_user_year.reset_index(drop=True,inplace=True)

    aggugyb = pd.DataFrame(agg_user_year.groupby("brand")["transation_count"].sum())
    aggugyb.reset_index(inplace=True)
    fig_user = px.bar(aggugyb, x="brand",y="transation_count",title= f"{year} Brand and Transation_count",width=400,color_discrete_sequence=px.colors.sequential.haline_r)
    st.plotly_chart(fig_user)
    return agg_user_year

def agg_user_Qauter(df, quater):
    agg_user_Q = df[df["Quater"] == quater]
    agg_user_Q.reset_index(drop=True,inplace=True)
    aggugQ = pd.DataFrame(agg_user_Q.groupby("brand")["transation_count",].sum())
    aggugQ.reset_index(inplace=True)
    fig_users = px.bar(aggugQ, x="brand",y="transation_count",title=f"{quater} quater of Brand and Transation_count",width=400,color_discrete_sequence=px.colors.sequential.haline_r)
    st.plotly_chart(fig_users)

def agg_user_state(df,state):
    agg_user = df[df["State"] == state]
    agg_user.reset_index(drop=True,inplace=True)
    fig = px.line(agg_user, x="brand", y="transation_count", markers=True, hover_data="pencentage",title=f"{state} of   brands transation count pencentage")
    st.plotly_chart(fig)

def map_tran_discrit(df,state):
    trans_type = df[df["State"] == state]
    trans_type.reset_index(drop=True,inplace=True)
    amountsg =  trans_type.groupby("District")[["transation_count","transation_amount"]].sum()
    amountsg.reset_index(inplace=True)
    fig_1 = px.bar(amountsg,x="transation_amount",y="District",orientation="h",color_discrete_sequence=px.colors.sequential.Magenta_r, title=f"{state} of district and transation amount ")
    st.plotly_chart(fig_1)
    fig_2 = px.bar(amountsg,x="transation_count",y="District",orientation="h",color_discrete_sequence=px.colors.sequential.Magenta,title=f"{state} of district and transation count ")
    st.plotly_chart(fig_2)

def map_user_1(df,year):
    map_user_year = df[df["Year"] == year]
    map_user_year.reset_index(drop=True,inplace=True)

    mapugy   = map_user_year.groupby("State")[["register_user","appopen"]].sum()
    mapugy.reset_index(inplace=True)
    fig = px.line(mapugy, x="State", y=["register_user","appopen"],title=f"{year} Register User And App Opens" ,width=1000,height=600, markers=True)
    st.plotly_chart(fig)
    return map_user_year

def map_user_2(df,quater):
    map_user_Q = df[df["Quater"] == quater]
    map_user_Q.reset_index(drop=True,inplace=True)
    mapugq   = map_user_Q.groupby("State")[["register_user","appopen"]].sum()
    mapugq.reset_index(inplace=True)
    fig = px.line(mapugq, x="State", y=["register_user","appopen"],title=f"{df["Year"].min()} of  {quater}  Register User And App Opens" ,color_discrete_sequence=px.colors.sequential.Hot_r,width=1000,height=600,markers=True)
    st.plotly_chart(fig)
    return map_user_Q

def map_user_3(df,state):
    map_user_S = df[df["State"] ==state]
    map_user_S.reset_index(drop=True,inplace=True)
    col1,col2 = st.columns(2)
    with col1:
        fig_state = px.bar(map_user_S, x="register_user", y="District", orientation = "h",
        title=f"{state} of Register User and District ",width=300, height=600, )
        st.plotly_chart(fig_state)
    with col2:
        fig_states = px.bar(map_user_S, x="appopen", y="District", orientation = "h",
        title=f"{state} of appopen User and District ", width=300, height=600,)
        st.plotly_chart(fig_states)

def top_tran_S(df,state):
    top_trans_s = df[df["State"] == state]
    top_trans_s.reset_index(drop=True,inplace=True)
    mapugs = top_trans_s.groupby("pincode")[["transation_count","transation_amount"]].sum()
    mapugs.reset_index(inplace=True)
    fig_state = px.bar(top_trans_s, x="Quater", y="transation_amount", hover_data="pincode" ,
    title="transation amount  and District Picodes ", height=600, color_discrete_sequence=px.colors.sequential.Rainbow)
    st.plotly_chart(fig_state) 
    fig_state = px.bar(top_trans_s, x="Quater", y="transation_count", hover_data="pincode" ,
    title="transation count  and District Picodes ", height=600, color_discrete_sequence=px.colors.sequential.Rainbow)
    st.plotly_chart(fig_state) 

def top_user_year(df,year):
    top_user_Y = df[df["Year"] == year]
    top_user_Y.reset_index(drop=True,inplace=True)
    topuy = pd.DataFrame(top_user_Y.groupby(["State", "Quater"])["register_user"].sum())
    topuy.reset_index(inplace=True)
    fig_year = px.bar(topuy, x="State",y="register_user",title=f"{year} of register user state and quaters" , color="Quater",color_discrete_sequence=px.colors.sequential.Rainbow)
    st.plotly_chart(fig_year) 
    return top_user_Y

def top_user_s(df,state):
    top_user_q = df[df["State"] == state]
    top_user_q.reset_index(drop=True,inplace=True)

    fig_q = px.bar(top_user_q,x="Quater",y="register_user",color="register_user",hover_data="pincode",title=f"{state} Register user")
    st.plotly_chart(fig_q) 


def query1():
    query1 = """SELECT state, SUM(Transacion_amount) AS Transacion_amount 
                FROM agg_tran 
                GROUP BY state 
                ORDER BY Transacion_amount
                desc    
                limit 10;
                """
    cursor.execute(query1)
    lose = cursor.fetchall()

    df = pd.DataFrame(lose,columns=["State","Transation_amount"])
    st.write(df)



with st.sidebar:
    st.subheader(":purple[Phonepe] Data Visualization ")
    nav_option = st.sidebar.selectbox(
        "Menu",
        ["Home", "Data Exploration" ,"Top_chart"],
         
    )
    
if  nav_option == 'Home':
      st.title("Phonepe Data Visualization")

elif nav_option == "Data Exploration":
    nav_option2 = st.sidebar.selectbox(
        "Data Visualization",
        ["Aggregate", "Map","Top"],
         
    )
    if nav_option2 == "Aggregate":
        st.title("Phonepe Data Visualization")
        st.subheader("Aggregate Transation")
        with st.sidebar:
            agg_options = st.sidebar.selectbox(
            "select the Transation",
            ["agg_tran", "agg_user"],
            
        )
        if agg_options == "agg_tran":
            
            col1 , col2 = st.columns(2)
            with col1:
                years = st.selectbox("Select the year", list(range(data["Year"].min(), data["Year"].max() + 1)))
            trans_y = tran_count_amount(data, years)
            qouter = st.selectbox("select the Quaters",list(range(trans_y["Quater"].min(),trans_y["Quater"].max() + 1 )))
            tran_count_amount_quater(trans_y ,qouter)
            col1 , col2 = st.columns(2)
            with col1:
                states = st.selectbox("select the state", trans_y["State"].unique())
            agg_tran_type(trans_y,states)

        elif agg_options == "agg_user":
            col1 , col2 = st.columns(2)
            with col1:
                years = st.slider("select the year",agg_user["Year"].min(),agg_user["Year"].max(),)
            trans_y = agg_user_tran_bran(agg_user,years)
            
            col1 , col2 = st.columns(2)
            with col1:
                qouter = st.slider("select the Quater",trans_y["Quater"].min(),trans_y["Quater"].max(),)
            agg_user_Qauter(trans_y ,qouter)

            col1 , col2 = st.columns(2)
            with col1:
                states = st.selectbox("select the state", trans_y["State"].unique())
            agg_user_state(trans_y,states)

    elif nav_option2 == "Map":
        st.subheader("Map Analaysis")
        with st.sidebar:
            Maps = st.sidebar.selectbox(
            "Menu",
            ["map_trans", "Map_user"],
            
        )
        if Maps == "map_trans":
            
            col1 , col2 = st.columns(2)
            with col1:
                years = st.slider("select the year",map_tran["Year"].min(),map_tran["Year"].max(),)
            trans_y = tran_count_amount(map_tran,years)
            
            col1 , col2 = st.columns(2)
            with col1:
                qouter = st.slider("select the Quater",trans_y["Quater"].min(),trans_y["Quater"].max(),)
            tran_count_amount_quater(trans_y ,qouter)
                
            col1 , col2 = st.columns(2)
            with col1:
                states = st.selectbox("select the State_map",trans_y["State"].unique())
            map_tran_discrit(trans_y ,states)
    
        if Maps == "Map_user":

            col1 , col2 = st.columns(2)
            with col1:
                years = st.slider("select the year",map_user["Year"].min(),map_user["Year"].max(),)
            map_user_y = map_user_1(map_user,years)

            col1 , col2 = st.columns(2)
            with col1:
                qouter = st.slider("select the Quater",map_user_y["Quater"].min(),map_user_y["Quater"].max(),)
            map_user_Q = map_user_2(map_user_y ,qouter)

            col1 , col2 = st.columns(2)
            with col1:
                states = st.selectbox("select the State_user",map_user_Q["State"].unique())
            map_user_3(map_user_Q ,states)
        
    elif  nav_option2 == "Top":
        st.subheader("Map Analaysis")
        with st.sidebar:
            top = st.sidebar.selectbox(
            "Select the Transation",
            ["Top_tran", "Top_user"],
            
        )
        if top == "Top_tran":
            col1 , col2 = st.columns(2)
            with col1:
                years = st.selectbox("select the year",list(range(top_tran["Year"].min(),top_tran["Year"].max(), + 1 )))
            top_trans_y = tran_count_amount(top_tran,years)

            col1 , col2 = st.columns(2)
            with col1:
                states = st.selectbox("select the State_users",top_trans_y["State"].unique())
            top_tran_S(top_trans_y ,states)

            col1 , col2 = st.columns(2)
            with col1:
                    qouter = st.slider("select the Quater",top_trans_y["Quater"].min(),top_trans_y["Quater"].max(),)
            tran_count_amount_quater(top_trans_y ,qouter)
        else:
            years = st.selectbox("select the year",list(range(top_user["Year"].min(),top_user["Year"].max(), + 1 )))
            top_user_y = top_user_year(top_user,years)

            
            states = st.selectbox("select the State_user",top_user_y["State"].unique())
            top_user_s(top_user_y ,states)

else:
    st.subheader("Aggcate Transation Top 10 state and transation   count")
    query1()
        
    

            

        

        

        

