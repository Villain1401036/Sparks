import imp
from itertools import count
import json
from logging import log
import re
import requests
import csv
import pandas as pd
import numpy as np
import psycopg2
import psycopg2.extras as extras
import pandas.io.sql as sqlio
import os 
import glob

def alert(text):
    print(text)
    log(text)

#pull the data and store it in a specific location
def pull_data(url,dest_file):
    #pulls the data and store it in a file location
    try :
        res = requests.get(url)
        if res.status_code == 200:#on success
            try:
                open(dest_file , 'wb').write(res.content)
            except Exception as e:
                raise e
        elif res.status_code == 404:#not found
            alert("file not found. please check the url carefully")
        elif res.status_code/100 == 5: #bad gateway 
            alert("server is unavailable , bad gateway")
        else:
            alert("something went wrong")
    except Exception as e:
        raise e


def write_transformed(df , destfolder='/home/rahul/reddit/posts_transformed/', dest_file='posts_',writeformat="csv" ):
    try:
        df.replace("\n",'',regex=True,inplace=True)
        df.replace("\\n",'',regex=True,inplace=True)

        if writeformat == "parquet":
            df.to_parquet( destfolder+dest_file+".parquet" )
        else:
            df.to_csv( destfolder+dest_file+".csv",sep="|" , na_rep='' ,escapechar='\\', index=False ,quoting=csv.QUOTE_NONE ,quotechar="-")

    except Exception as e :
        raise e


def transformdata_raw(src_file,destfolder='/home/rahul/Sparks/data',dtype=None,table='users',colsorder=None):
    try:
        file = open(src_file)
        
        filedata  = json.load(file)
        
        if table == 'users':
                df_user = pd.json_normalize(filedata,sep='_')
                
                data = df_user
                print(data)
                print(data.columns)
                #clean the dataframe
                #change the date in timestamp format 
                data["createdAt"] =  data["createdAt"].astype('datetime64')
                data["updatedAt"] =  data["updatedAt"].astype('datetime64')
                data["birthDate"] =  data["birthDate"].astype('datetime64')
                
                #change the bool/float column to float  so that we can use it later // to category if the column is having a finite set of values
                data['profile_gender'] = data['profile_gender'].astype('category')
                data['profile_income'] = data['profile_income'].astype('float')
                data['country'] = data['country'].astype('category')
                data['city'] = data['city'].astype('category')
            


                data = data.rename(columns={"profile_gender":"gender",'profile_income':'income','profile_isSmoking':'isSmoking','profile_profession':'profession'})
                
                #remove data where there is nan 
                data = data.dropna(subset=['id'])
                if type(colsorder) == str:
                    colsorder = colsorder.split("|")
                data = data[colsorder]
                write_transformed(data,destfolder,dest_file=table)

                #for bigquery transformed
                # remove the PII info from the csv
                df_filtered = data[[ 'id' ,'createdAt', 'birthdate' , 'city', 'country', 'email' , 'gender', 'issmoking' , 'income' ]]
                write_transformed(df_filtered,destfolder,dest_file=table+"_bq")
                print("user transformed")
                
        elif table == 'subscriptions':
                df_subs = pd.json_normalize(filedata , record_path=['subscription'],meta=['id'])
                data = df_subs
                #clean the dataframe
                #change the date in timestamp format 
                data["createdAt"] =  data["createdAt"].astype('datetime64')
                data["startDate"] =  data["startDate"].astype('datetime64')
                data["endDate"] =  data["endDate"].astype('datetime64')
                
                #change the bool/float column to float  so that we can use it later // to category if the column is having a finite set of values
                data['amount'] = data['amount'].astype('float')
                data['status'] = data['status'].astype('category')
            
                data = data.rename(columns={"id":"user_id"})
                #remove data where there is nan 
                data = data.dropna(subset=['user_id'])


                if type(colsorder) == str:
                    colsorder = colsorder.split("|")
                data = data[colsorder]
                write_transformed(data,destfolder,dest_file=table)
                
        elif table == 'messages':
                df_mess = pd.json_normalize(filedata , sep="_")
                data = df_mess
                #clean the dataframe
                #change the date in timestamp format 
                data["createdAt"] =  data["createdAt"].astype('datetime64')
                
                #change the bool/float column to float  so that we can use it later // to category if the column is having a finite set of values
                data['message'] = data['message'].astype('string')


                # data = data.rename(columns={"id":"message_id"})
                #remove data where there is nan 
                data = data.dropna(subset=['id'])

                if type(colsorder) == str:
                    colsorder = colsorder.split("|")
                data = data[colsorder]
                write_transformed(data,destfolder,dest_file=table)

                #for bigquery transformed
                # remove the PII info from the csv
                df_filtered = data[['id' , 'senderid' , 'createdat' ]]
                write_transformed(df_filtered,destfolder,dest_file=table+"_bq")
                print("user transformed")
                

    except Exception as e:
        print(e)
    finally :
        file.close()


def execute_insert_bulk(conn, table='users' , insert_cols=[],filename=None):
  
    cols = ','.join(insert_cols)

    query = """COPY %s( %s )
        FROM '%s'
        DELIMITER '|'
        CSV HEADER;""" % (table, cols,filename)

    cursor = conn.cursor()

    try:
        # extras.execute_values(cursor, query, tuples)
        cursor.execute(query)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("the dataframe is inserted")
    cursor.close()


def insert_to_postgres(conn,src_file,table='users',insertcols=None):
    #create a conn to the database
    try:
        # conn = psycopg2.connect(database="redditdatabase", user='rahul', password='pass', host='127.0.0.1', port='5432')
        if conn == None:
            conn = psycopg2.connect(database="postgres", user='rahul', password='Cherry@07', host='127.0.0.1', port='5432')
        if conn:
            print("connection to postgres successful", conn)

        #read the csv file and insert it into database

        if type(insertcols) == str:
            insertcols = insertcols.split("|")
        execute_insert_bulk(conn ,table=table, filename=src_file,insert_cols=insertcols )
        
    except Exception as e:
        raise e
    finally :
        conn.close()




