
-- """hiveSQL_create_foreign_cards_table
-- hiveSQL query for creating the mtg_foreign_cards table.
-- """
-- hiveSQL_create_foreign_cards_table='''
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
--'''