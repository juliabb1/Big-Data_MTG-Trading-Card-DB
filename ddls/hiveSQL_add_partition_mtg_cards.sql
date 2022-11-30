--"""hiveSQL_add_partition_mtg_cards
--hiveSQL query to create a verisioning of the mtg_cards.
--"""
--hiveSQL_add_partition_mtg_cards='''
ALTER TABLE mtg_cards
ADD IF NOT EXISTS partition(partition_year={{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}, partition_month={{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}, partition_day={{ macros.ds_format(ds, "%Y-%m-%d", "%d")}})
LOCATION '/user/hadoop/mtg/raw/mtg_cards/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}';
--'''
