# -*- coding: utf-8 -*-
# 只适用于迁移数据源的表结构，使用show create table查到建表语句并在目标库执行,保持库和表名保持一致
import re
import pymysql
from util.mysql_wrapper import MySQLWrapper

class MigrateTable():

    def __init__(self,src_source_id, dest_source_id, table_list=[]):

        self.src_source_id=src_source_id
        self.dest_source_id=dest_source_id
        self.table_list=table_list
        self.mysql_connect = MySQLWrapper()

    # 根据source_id从数据库中获取数据源的连接信息
    def get_source_conn(self, source_id):
        self.mysql_connect.connect()
        sql = f"select source_url,source_port,source_database,source_user,source_passwd from arsenal_data_source where source_id='{source_id}'"
        source_data = self.mysql_connect.fetch_data(sql)

        if source_data:
            source_url=source_data[0]["source_url"]
            source_port=int(source_data[0]["source_port"])
            source_database=source_data[0]["source_database"]
            source_user=source_data[0]["source_user"]
            source_passwd=source_data[0]["source_passwd"]
            source_conn = pymysql.connect(host=source_url, port=source_port, user=source_user, password=source_passwd, database=source_database, charset='utf8')
            return source_conn
        else:
            print(f"数据源[{source_id}]不存在!")
            return False
        
    # 迁移表
    def migrate_table(self):

        src_source_conn = self.get_source_conn(self.src_source_id)
        dest_source_conn = self.get_source_conn(self.dest_source_id)

        if not src_source_conn:
            print(f"获取来源数据源[{self.src_source_id}]的MySQL连接失败!")
            return False
        elif not dest_source_conn:
            print(f"获取目标数据源[{self.dest_source_id}]的MySQL连接失败!")
            return False

        for table_name in self.table_list:
            with src_source_conn.cursor() as cursor:
                cursor.execute(f"show create table `{table_name}`;")
                result=cursor.fetchall()
                if len(result) == 0:
                    print(f"数据源[{self.src_source_id}]中获取表[{table_name}]的建表语句失败!")
                    return False

                else:
                    create_table_sql=result[0][1]

            with dest_source_conn.cursor() as cursor:
                cursor.execute(f"show tables like '{table_name}';")
                result=cursor.fetchall()
                if len(result) == 0:
                    # 将自增ID设置为0
                    create_table_sql = re.sub(r'AUTO_INCREMENT=\d*', r'AUTO_INCREMENT=0', create_table_sql)
                    cursor.execute(create_table_sql)
                    dest_source_conn.commit()
                    print(f"目的数据源[{self.dest_source_id}]中表[{table_name}]已创建!")
                    return True
                else:
                    print(f"目的数据源[{self.dest_source_id}]中表[{table_name}]已存在!")
                    return True

if __name__ == "__main__":

    # 样例
    # 同步全库
    #a = MigrateTable('src_source_id', 'dest_source_id').main()
    # 同步部分表
    #a = MigrateTable('src_source_id', 'dest_source_id',["table1","table2","table3"]).main()
