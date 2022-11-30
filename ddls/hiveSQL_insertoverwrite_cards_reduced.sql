-- """hiveSQL_insertoverwrite_cards_reduced
-- hiveSQL query to join mtg_foreign_cards table with mtg_cards table and insert it into the
-- cards_reduced table. Cards reduced table will only contain the columns name, multiverseid and imageUrl for
-- German cards.
-- """
-- hiveSQL_insertoverwrite_cards_reduced='''
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
-- '''