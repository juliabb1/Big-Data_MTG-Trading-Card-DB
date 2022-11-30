--"""mySQL_drop_mtg_cards_table
--Drops the mysql mtg_cards table if it exists.
--This table containes cards_reduced from the Hive Table
--"""
--query = '''
DROP TABLE IF EXISTS mtg_cards;
--'''