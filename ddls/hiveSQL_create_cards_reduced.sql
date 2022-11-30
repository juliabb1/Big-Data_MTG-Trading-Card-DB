-- """hiveSQL_create_cards_reduced
-- hiveSQL query to create the cards_reduced table and store it in '/user/hadoop/mtg/final/cards'.
-- """
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
-- '''