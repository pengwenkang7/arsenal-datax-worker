# -*- coding: utf-8 -*-
# 只适用于迁移数据源的表结构，使用show create table查到建表语句并在目标库执行,保持库和表名保持一致
import os, sys
sys.path.append(f'{os.getcwd()}')

import re
import pymysql
from util.mysql_wrapper import MySQLWrapper

class MigrateTable():

    def __init__(self,src_source_id, dest_source_id, table_mapping_list=[]):
        self.src_source_id=src_source_id
        self.dest_source_id=dest_source_id
        self.table_mapping_list=table_mapping_list
        self.mysql_connect=MySQLWrapper()

    # 根据source_id从数据库中获取数据源的连接信息
    def get_source_conn(self, source_id):
        self.mysql_connect.connect()
        sql = f"SELECT source_url,source_port,source_database,source_user,source_passwd FROM arsenal_data_source WHERE source_id='{source_id}'"
        source_data = self.mysql_connect.fetch_data(sql)

        if source_data:
            source_url=source_data[0]["source_url"]
            source_port=int(source_data[0]["source_port"])
            source_database=source_data[0]["source_database"]
            source_user=source_data[0]["source_user"]
            source_passwd=source_data[0]["source_passwd"]
            return source_url,source_port,source_database,source_user,source_passwd            
        else:
            print(f"数据源[{source_id}]不存在!")
            return False

    # 迁移表
    def migrate_table(self):

        src_source_url, src_source_port, src_source_database, src_source_user, src_source_passwd = self.get_source_conn(self.src_source_id)
        dest_source_url, dest_source_port, dest_source_database, dest_source_user, dest_source_passwd = self.get_source_conn(self.dest_source_id)

        try:
            src_source_conn=pymysql.connect(host=src_source_url, port=src_source_port, user=src_source_user, password=src_source_passwd, database=src_source_database, charset='utf8')
        except Exception as e:
            print(f"获取源数据库连接失败! error:[{e}]")
            return False
        # 检查目的数据库是否存在，不存在则创建
        try:
            tmp_dest_source_conn = pymysql.connect(host=dest_source_url, port=dest_source_port, user=dest_source_user, password=dest_source_passwd, charset='utf8')
            with tmp_dest_source_conn.cursor() as cursor:
                cursor.execute(f"SHOW DATABASES LIKE \"{dest_source_database}\";")
                result=cursor.fetchall()
                if len(result) == 0:
                    cursor.execute(f"CREATE DATABASE `{dest_source_database}` DEFAULT CHARACTER SET UTF8")
            dest_source_conn = pymysql.connect(host=dest_source_url, port=dest_source_port, user=dest_source_user, password=dest_source_passwd, database=dest_source_database, charset='utf8')
        except Exception as e:
            print(f"获取目的数据库连接失败! error:[{e}]")
            return False

        if len(self.table_mapping_list) == 0:
            with src_source_conn.cursor() as cursor:
                cursor.execute("SHOW FULL TABLES WHERE Table_type = 'BASE TABLE';")
                result=cursor.fetchall()

                for t in result:
                    self.table_mapping_list.append(f"{t[0]}:{t[0]}")

        fail_create_table_list = []
        for table_mapping in self.table_mapping_list:
            if ":" in table_mapping:
                table_mapping = table_mapping.replace(" ", "").replace("\t", "").replace("\n", "")
                src_table=table_mapping.split(":")[0]
                dest_table=table_mapping.split(":")[1]
            else:
                print(f"表关系映射列表中[{table_mapping}]配置错误!")
                continue
            try:
                with src_source_conn.cursor() as cursor:
                    cursor.execute(f"SHOW CREATE TABLE {src_table};")
                    result=cursor.fetchall()
                    if len(result) == 0:
                        print(f"数据源[{self.src_source_id}]中获取表[{src_table}]的建表语句失败!")
                        return False
                    else:
                        create_table_sql=result[0][1]

                with dest_source_conn.cursor() as cursor:
                    cursor.execute(f"SHOW TABLES LIKE '{dest_table}';")
                    result=cursor.fetchall()
                    if len(result) == 0:
                        # 将自增ID设置为0
                        create_table_sql = re.sub(r'AUTO_INCREMENT=\d*', r'AUTO_INCREMENT=0', create_table_sql)
                        # 替换建表语句中的表名称
                        create_table_sql = re.sub(fr"CREATE TABLE `{src_table}`", fr"CREATE TABLE `{dest_table}`", create_table_sql)
                        print(create_table_sql)
                        cursor.execute(create_table_sql)
                        dest_source_conn.commit()
                        print(f"数据源[{self.dest_source_id}]中表[{dest_table}]已创建, 与源表[{src_table}]结构一致!")
                    else:
                        print(f"数据源[{self.dest_source_id}]中表[{dest_table}]已存在!")
            except Exception as e:
                fail_create_table_list.append(src_table)
                print(f"数据源[{self.dest_source_id}]中表[{dest_table}]创建失败 error: [{e}]")

        if len(fail_create_table_list) == 0:
            return True
        else:
            return False

if __name__ == "__main__":

    # 样例
    # 同步全库
    # a = MigrateTable('src_source_id', 'dest_source_id').main()
    # 同步部分表
    # a = MigrateTable('src_source_id', 'dest_source_id',["table1","table2","table3"]).main()

