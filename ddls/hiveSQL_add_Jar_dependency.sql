--"""hiveSQL_add_Jar_dependency
-- hiveSQL query to add a JAR dependency to overwrite hive table.
--"""
--hiveSQL_add_Jar_dependency='''
ADD JAR /home/hadoop/hive/lib/hive-hcatalog-core-3.1.2.jar;
--'''