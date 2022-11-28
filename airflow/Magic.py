# -*- coding: utf-8 -*-
"""
author: Julia Bai
date: 28.11.2022
license: free
"""

from datetime import datetime
import requests
import json
import sys
import subprocess
from pyhive import hive

import pymysql
import paramiko
import pandas as pd
from paramiko import SSHClient
from sshtunnel import SSHTunnelForwarder

import mysql.connector

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.http_download_operations import HttpDownloadOperator
from airflow.operators.zip_file_operations import UnzipFileOperator
from airflow.operators.hdfs_operations import HdfsPutFileOperator, HdfsGetFileOperator, HdfsMkdirFileOperator
from airflow.operators.filesystem_operations import CreateDirectoryOperator
from airflow.operators.filesystem_operations import ClearDirectoryOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'airflow'
}

# HiveSQL ################################
"""hiveSQL_create_mtg_cards_table
hiveSQL query for creating the mtg_cards table without mtg foreign cards.
"""
hiveSQL_create_mtg_cards_table='''
CREATE EXTERNAL TABLE IF NOT EXISTS mtg_cards(
    id STRING,
	name STRING,
    manaCost STRING,
	cmc FLOAT,
    colors ARRAY<STRING>,
	colorIdentity ARRAY<STRING>,
    type STRING,
    types ARRAY<STRING>,
    subtypes ARRAY<STRING>,
    rarity STRING,
    setName STRING,
	text STRING,
	flavor STRING,
	artist STRING,
    power STRING,
	toughness STRING,
	layout STRING,
    multiverseid STRING,
    imageUrl STRING,
    variations ARRAY<STRING>,
	printings ARRAY<STRING>,
	originalText STRING,
	originalType STRING,
	legalities ARRAY<STRUCT<format:STRING, legality:STRING>>,
	names ARRAY<STRING>
    ) PARTITIONED BY (partition_year int, partition_month int, partition_day int) 
    ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' 
    LOCATION 'hdfs:///user/hadoop/mtg/raw/mtg_cards';
'''

"""hiveSQL_create_foreign_cards_table
hiveSQL query for creating the mtg_foreign_cards table.
"""
hiveSQL_create_foreign_cards_table='''
CREATE EXTERNAL TABLE IF NOT EXISTS mtg_foreign_cards(
	name STRING,
    text STRING,
    type STRING,
    flavor STRING,
    imageUrl STRING,
    language STRING,
    multiverseid STRING,
    cardid STRING
) PARTITIONED BY (partition_year int, partition_month int, partition_day int) 
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' 
LOCATION 'hdfs:///user/hadoop/mtg/raw/mtg_foreign_cards'; 
'''

"""hiveSQL_add_partition_mtg_cards
hiveSQL query to create a verisioning of the mtg_cards.
"""
hiveSQL_add_partition_mtg_cards='''
ALTER TABLE mtg_cards
ADD IF NOT EXISTS partition(partition_year={{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}, partition_month={{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}, partition_day={{ macros.ds_format(ds, "%Y-%m-%d", "%d")}})
LOCATION '/user/hadoop/mtg/raw/mtg_cards/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}';
'''

"""hiveSQL_add_partition_mtg_cards
hiveSQL query to create a verisioning of the mtg_foreign_cards.
"""
hiveSQL_add_partition_mtg_foreign_cards='''
ALTER TABLE mtg_foreign_cards
ADD IF NOT EXISTS partition(partition_year={{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}, partition_month={{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}, partition_day={{ macros.ds_format(ds, "%Y-%m-%d", "%d")}})
LOCATION '/user/hadoop/mtg/raw/mtg_foreign_cards/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}';
'''

"""hiveSQL_add_Jar_dependency
hiveSQL query to add a JAR dependency to overwrite hive table.
"""
hiveSQL_add_Jar_dependency='''
ADD JAR /home/hadoop/hive/lib/hive-hcatalog-core-3.1.2.jar;
'''

"""hiveSQL_drop_cards_table
hiveSQL query to drop the mtg_cards table.
"""
hiveSQL_drop_cards_table='''
DROP TABLE IF EXISTS mtg_cards;
'''

"""hiveSQL_drop_foreign_cards_table
hiveSQL query to drop the mtg_foreign_cards table.
"""
hiveSQL_drop_foreign_cards_table='''
DROP TABLE IF EXISTS mtg_foreign_cards;
'''

"""hiveSQL_drop_reduced_cards_table
hiveSQL query to drop the reduced_cards table
"""
hiveSQL_drop_reduced_cards_table='''
DROP TABLE IF EXISTS cards_reduced;
'''

"""hiveSQL_create_cards_reduced
hiveSQL query to create the cards_reduced table and store it in '/user/hadoop/mtg/final/cards'.
"""
hiveSQL_create_cards_reduced='''
CREATE EXTERNAL TABLE IF NOT EXISTS cards_reduced (
    name STRING,
    multiverseid STRING,
    imageUrl STRING
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = '"'
    )
STORED AS TEXTFILE LOCATION '/user/hadoop/mtg/final/cards';
'''

"""hiveSQL_insertoverwrite_cards_reduced
hiveSQL query to join mtg_foreign_cards table with mtg_cards table and insert it into the
cards_reduced table. Cards reduced table will only contain the columns name, multiverseid and imageUrl for
German cards.
"""
hiveSQL_insertoverwrite_cards_reduced='''
ADD JAR /home/hadoop/hive/lib/hive-hcatalog-core-3.1.2.jar;
INSERT OVERWRITE TABLE cards_reduced
SELECT
    c.name,
    c.multiverseid,
    f.imageUrl
FROM
    mtg_cards c
    JOIN mtg_foreign_cards f ON (c.id = f.cardid)
WHERE
    f.language = "German";
'''

# DOWNLOAD MTG CARDS ################################
"""get_all_mtg_card
Iterates through all available mtg cards the magicgathering-api provides.
The English Cards will be written in a "mtg_cards_*.json" file and the Foreign Cards will be written in a
a "mtg_foreign_cards_*.json" file in the folder "/home/airflow/mtg"
"""
def get_all_mtg_cards():
    # get first card
    res = requests.get("https://api.magicthegathering.io/v1/cards?pageSize=150&page=1")
    total_count_pages = int(res.headers["Total-Count"])

    print("1 von " + str(709))
    cards = res.json()["cards"]
    foreign_cards = getForeignCards(cards)
    
    for i in range(2, 710): # pages are filled until page 709
        print(str(i) + " von " + str(709))
        res = requests.get("https://api.magicthegathering.io/v1/cards?pageSize=1&page=" + str(i))
        res_cards = res.json()["cards"]
        foreign_cards = foreign_cards + getForeignCards(res_cards)
        cards = cards + res.json()["cards"]

    # Cleans the cards variable, so that only English mtg cards are saved in it
    for i in range(len(cards)):
        if "foreignNames" in cards[i]:
            del cards[i]["foreignNames"]
    
    # Writes the gathered card information into a json file
    cardsJson = toJSON(cards)
    text_file = open("/home/airflow/mtg/mtg_cards_" + getDate("all") + ".json", "w")
    text_file.write(cardsJson)

    foreignCardsJson = toJSON(foreign_cards)
    text_file = open("/home/airflow/mtg/mtg_foreign_cards_" + getDate("all") + ".json", "w")
    text_file.write(foreignCardsJson)
    return

"""getForeignCards
Iterates through every key in the cards-json-object, searching for the "foreignNames" key.
All available foreign cards data are being appended to a foreignCards list.

params:
    cards (JSON): Contains all information about a card
"""
def getForeignCards(cards): 
    foreignCards = []
    for card in cards:
        if "foreignNames" in card:
            for foreignCard in card["foreignNames"]:
                foreignCard["cardid"] = card["id"]
                foreignCards.append(foreignCard)
    return foreignCards

"""toJSON
Converts a list into a JSON Object.

params:
    cards (List): Contains all information about a card
"""
def toJSON(cards):
    for i in range(len(cards)):
        cards[i] = json.dumps(cards[i])
    cardsJson = ",\n".join(cards)
    return cardsJson

"""getDate
Get an element of the current date.

params:
    entity (string): contains information which element of the current date should be returned
    
returns:
    date: element of the current date
"""
def getDate(entity):
    if entity=="y":
        return datetime.today().strftime('%Y')
    if entity=="m":
        return datetime.today().strftime('%m')
    if entity=="d":
        return datetime.today().strftime('%d')
    if entity=="all":
        return datetime.today().strftime('%Y-%m-%d')

# MYSQL ################################
"""execute_mysql_ssh_query
Executes a mysql query via ssh tunnel in a predefined database.
params:
    query (string): Contains the MySQL Query
    database_name (string): Name of the database the query should be executed in
    data (list, tuple): Contains data
"""
def execute_mysql_ssh_query(query, database_name, data=None):
    mypkey = paramiko.RSAKey.from_private_key_file('/home/airflow/airflow/dags/big-data')
    # if you want to use ssh password use - ssh_password='your ssh password', bellow
    sql_hostname = '172.17.0.2'
    sql_username = 'root'
    sql_password = 'password'
    sql_main_database = database_name
    sql_port = 3306
    ssh_host = '34.89.121.166'
    ssh_user = 'baijulia02'
    ssh_port = 22
 
    with SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_username=ssh_user,
        ssh_pkey=mypkey,
        remote_bind_address=(sql_hostname, sql_port)) as tunnel:
        mysql_conn = pymysql.connect(host='127.0.0.1', user=sql_username,
                passwd=sql_password, db=sql_main_database,
                port=tunnel.local_bind_port)
        cursor = mysql_conn.cursor()
        if data!=None:
            # if data is given
            cursor.executemany(query, data)
            # commit the changes
            mysql_conn.commit()
        else:
            cursor.execute(query)
        cursor.close()
        mysql_conn.close()

"""create_mysql_mtg_enduser_db
Creates a mysql database if it doesn't exist.
"""
def create_mysql_mtg_enduser_db():
    execute_mysql_ssh_query(query="CREATE DATABASE IF NOT EXISTS mtg;", database_name="")

"""create_mysql_mtg_cards_table
Creates a mysql mtg_cards table if it doesn't exist.
This table containes cards_reduced from the Hive Table
"""
def create_mysql_mtg_cards_table():
    query = '''CREATE TABLE IF NOT EXISTS mtg_cards (
        name VARCHAR(60),
        multiverseid VARCHAR(10),
        imageUrl VARCHAR(150)
    );'''
    execute_mysql_ssh_query(query, database_name="mtg")

"""mySQL_drop_mtg_cards_table
Drops the mysql mtg_cards table if it exists.
This table containes cards_reduced from the Hive Table
"""
def mySQL_drop_mtg_cards_table():
    query = '''DROP TABLE IF EXISTS mtg_cards;'''
    execute_mysql_ssh_query(query, "mtg")


# HIVE TO MYSQL ################################
"""fetch_hive_table_data
Creates a ssh tunnel to connect to the hive database.
Fetches the data of the hive_table "cards_reduced" in database "default" and returns the selected data.

returns:
    data (list, tuple, optional): Contains selected data from the hive table
"""
def fetch_hive_table_data(query):
    mypkey = paramiko.RSAKey.from_private_key_file('/home/airflow/airflow/dags/big-data')
    # if you want to use ssh password use - ssh_password='your ssh password', bellow
    ssh_host = '34.89.121.166'
    ssh_user = 'baijulia02'
    ssh_port = 22
 
    hive_host = "172.17.0.1"
    hive_port = 10000
    hive_user = "hadoop"
    
    with SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_username=ssh_user,
        ssh_pkey=mypkey,
        remote_bind_address=(hive_host, hive_port)) as tunnel:
            hive_conn = hive.Connection(
            host="127.0.0.1", 
            port=tunnel.local_bind_port,
            username=hive_user
            )
            cursor = hive_conn.cursor()
            query = "SELECT * FROM default.cards_reduced"
            cursor.execute(query)
            return cursor.fetchall()

"""load_data_into_mySQL_mtg_cards_table
Data will be exported from Hive to MySQL.
Data in hiveSQL "cards_reduced" table will be fetched and inserted 
into the "mtg_cards" table in the MySQL database.
"""
def load_data_into_mySQL_mtg_cards_table():
    hive_fetch_query = "SELECT * FROM default.cards_reduced"
    hive_data = fetch_hive_table_data(hive_fetch_query)
    
    mysql_insert_query = '''INSERT INTO mtg_cards(name, multiverseid, imageurl) VALUES (%s, %s, %s)'''
    execute_mysql_ssh_query(query=mysql_insert_query, database_name="mtg", data=hive_data)
    

# DAG ################################
"""dag
Declares a DAG.
The Directed Acyclic Graph collects the Tasks and 
organize their dependencies and relationships.
"""
dag = DAG('Magic', default_args=args, description='DAG for mtg cards import',
          schedule_interval='56 18 * * *',
          start_date=datetime(2019, 10, 16), catchup=False, max_active_runs=1)

"""create_local_import_dir
Task to create the directory "mtg" in "home/airflow"-
"""
create_local_import_dir = CreateDirectoryOperator(
    task_id='create_import_directory',
    path='/home/airflow',
    directory='mtg',
    dag=dag,
)

"""clear_local_import_dir
Task to empty the directory "mtg" in "home/airflow".
"""
clear_local_import_dir = ClearDirectoryOperator(
    task_id='clear_import_directory',
    directory='/home/airflow/mtg',
    pattern='*',
    dag=dag,
)

"""download_mtg_cards
Task to download all mtg_cards and mtg_foreign_cards.
"""
download_mtg_cards = PythonOperator(
    task_id='download_mtg_cards',
    python_callable = get_all_mtg_cards,
    op_kwargs = {},
    dag=dag
)

"""create_mtg_cards_partition_dir
Task to create a directory in the hdfs for the mtg_cards.
"""
create_mtg_cards_partition_dir = HdfsMkdirFileOperator(
    task_id='mkdir_hdfs_cards_dir',
    directory='/user/hadoop/mtg/raw/mtg_cards/' + getDate("y") + "/" + getDate("m") + "/" + getDate("d"),
    hdfs_conn_id='hdfs',
    dag=dag,
)

"""create_mtg_foreign_cards_partition_dir
Task to create a directory in the hdfs for the mtg_foreign_cards.
"""
create_mtg_foreign_cards_partition_dir = HdfsMkdirFileOperator(
    task_id='mkdir_hdfs_foreign_cards_dir',
    directory='/user/hadoop/mtg/raw/mtg_foreign_cards/' + getDate("y") + "/" + getDate("m") + "/" + getDate("d"),
    hdfs_conn_id='hdfs',
    dag=dag,
)

"""hdfs_put_cards
Task to put the mtg_cards json files from airflow 
into the hadoop file system.
"""
hdfs_put_mtg_cards = HdfsPutFileOperator(
    task_id='upload_mtg_cards_to_hdfs',
    local_file='/home/airflow/mtg/mtg_cards_' + getDate('all') +'.json',
    remote_file='/user/hadoop/mtg/raw/mtg_cards/'+getDate("y") + "/" + getDate("m") + "/" + getDate("d")+ '/' + 'mtg_cards_' + getDate("all")+ '.json',
    hdfs_conn_id='hdfs',
    dag=dag,
)

"""hdfs_put_foreign_cards
Task to put the mtg_foreign_cards json files from airflow 
into the hadoop file system.
"""
hdfs_put_foreign_cards = HdfsPutFileOperator(
    task_id='upload_foreign_cards_to_hdfs',
    local_file='/home/airflow/mtg/mtg_foreign_cards_' + getDate('all') +'.json',
    remote_file='/user/hadoop/mtg/raw/mtg_foreign_cards/' + getDate("y") + "/" + getDate("m") + "/" + getDate("d") + '/' + 'mtg_foreign_cards_' + getDate("all")+ '.json',
    hdfs_conn_id='hdfs',
    dag=dag,
)

"""delete_HiveTable_cards
Task to drop the hive table mtg_cards.
"""
delete_HiveTable_mtg_cards = HiveOperator(
    task_id='delete_mtg_cards_table',
    hql=hiveSQL_drop_cards_table,
    hive_cli_conn_id='beeline',
    dag=dag)

"""delete_HiveTable_foreign_cards
Task to drop the hive table mtg_foreign_cards.
"""
delete_HiveTable_foreign_cards = HiveOperator(
    task_id='delete_foreign_cards_table',
    hql=hiveSQL_drop_foreign_cards_table,
    hive_cli_conn_id='beeline',
    dag=dag)

"""delete_HiveTable_reduced_cards
Task to drop the hive table reduced_cards.
"""
delete_HiveTable_reduced_cards = HiveOperator(
    task_id='delete_reduced_cards_table',
    hql=hiveSQL_drop_reduced_cards_table,
    hive_cli_conn_id='beeline',
    dag=dag)

"""delete_MySQLTable_mtg_cards
Task to drop the MySQL table mtg_cards.
"""
delete_MySQLTable_mtg_cards = PythonOperator(
    task_id='delete_mysql_mtg_cards_table',
    python_callable = mySQL_drop_mtg_cards_table,
    op_kwargs = {},
    dag=dag
)
"""add_JAR_dependencies
Task to add Jar dependencies.
Those allow specific table manipulation.
"""
add_JAR_dependencies = HiveOperator(
    task_id='add_jar_dependencies',
    hql=hiveSQL_add_Jar_dependency,
    hive_cli_conn_id='beeline',
    dag=dag)

"""create_HiveTable_cards
Task to create the hive table mtg_cards
"""
create_HiveTable_mtg_cards = HiveOperator(
    task_id='create_mtg_cards_table',
    hql=hiveSQL_create_mtg_cards_table,
    hive_cli_conn_id='beeline',
    dag=dag)

"""create_HiveTable_foreign_cards
Task to create the hive table mtg_foreign_cards
"""
create_HiveTable_foreign_cards = HiveOperator(
    task_id='create_foreign_cards_table',
    hql=hiveSQL_create_foreign_cards_table,
    hive_cli_conn_id='beeline',
    dag=dag)

"""addPartition_HiveTable_mtg_cards
Task to add a partition into the hiveSQL table mtg_cards.
It will be partitioned after the date.
"""
addPartition_HiveTable_mtg_cards = HiveOperator(
    task_id='add_partition_mtg_cards_table',
    hql=hiveSQL_add_partition_mtg_cards,
    hive_cli_conn_id='beeline',
    dag=dag)

"""addPartition_HiveTable_mtg_foreign_cards
Task to add a partition into the hiveSQL table mtg_foreign_cards.
It will be partitioned after the date.
"""
addPartition_HiveTable_mtg_foreign_cards = HiveOperator(
    task_id='add_partition_mtg_foreign_cards_table',
    hql=hiveSQL_add_partition_mtg_foreign_cards,
    hive_cli_conn_id='beeline',
    dag=dag)

"""dummy_op
Task to add a dummy operator into the DAG.
"""
dummy_op = DummyOperator(
        task_id='dummy', 
        dag=dag)

"""create_HiveTable_cards_reduced
Task to create the hive table cards_reduced.
"""
create_HiveTable_cards_reduced = HiveOperator(
    task_id='create_cards_reduced',
    hql=hiveSQL_create_cards_reduced,
    hive_cli_conn_id='beeline',
    dag=dag)

"""hive_insert_overwrite_cards_reduced
Task to overwrite the hive table cards_reduced.
It will contain all the German mtg cards.
"""
hive_insert_overwrite_cards_reduced = HiveOperator(
    task_id='hive_write_cards_reduced_table',
    hql=hiveSQL_insertoverwrite_cards_reduced,
    hive_cli_conn_id='beeline',
    dag=dag)

"""mySQL_create_mtg_database
Task to create a mysql database called mtg.
"""
mySQL_create_mtg_database = PythonOperator(
    task_id='mySQL_create_mtg_database',
    python_callable = create_mysql_mtg_enduser_db,
    op_kwargs = {},
    dag=dag
)

"""mySQL_create_mtg_cards_table
Task to create a mysql table called mtg_cards.
"""
mySQL_create_mtg_cards_table = PythonOperator(
    task_id='mySQL_create_mtg_cards_table',
    python_callable = create_mysql_mtg_cards_table,
    op_kwargs = {},
    dag=dag
)

"""mySQL_create_mtg_cards_table
Task to load the data from the hive table mtg_reduced into the
mySQL table mtg_cards.
"""
load_data_hive_to_mysql_mtg_cards = PythonOperator(
    task_id='load_data_hive_to_mysql_mtg_cards',
    python_callable = load_data_into_mySQL_mtg_cards_table,
    op_kwargs = {},
    dag=dag
)

"""
Airflow. Workflow.
"""
create_local_import_dir >> clear_local_import_dir >> download_mtg_cards
download_mtg_cards >> add_JAR_dependencies >> create_mtg_cards_partition_dir >> hdfs_put_mtg_cards >> delete_HiveTable_mtg_cards >> create_HiveTable_mtg_cards >> addPartition_HiveTable_mtg_cards >> dummy_op
download_mtg_cards >> add_JAR_dependencies >> create_mtg_foreign_cards_partition_dir >> hdfs_put_foreign_cards >> delete_HiveTable_foreign_cards >> create_HiveTable_foreign_cards >> addPartition_HiveTable_mtg_foreign_cards >> dummy_op
dummy_op >> delete_HiveTable_reduced_cards >> create_HiveTable_cards_reduced >> hive_insert_overwrite_cards_reduced >> mySQL_create_mtg_database  >> delete_MySQLTable_mtg_cards >> mySQL_create_mtg_cards_table >> load_data_hive_to_mysql_mtg_cards
