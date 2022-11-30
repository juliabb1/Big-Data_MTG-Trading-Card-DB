-- """hiveSQL_create_mtg_cards_table
-- hiveSQL query for creating the mtg_cards table without mtg foreign cards.
-- """
-- hiveSQL_create_mtg_cards_table='''
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
--'''