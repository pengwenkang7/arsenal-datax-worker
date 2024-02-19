# -*- coding: utf-8 -*-
# 用于快速生成任务, 适用于整库不做变动进行迁移备份
import os, sys
sys.path.append(f'{os.getcwd()}')

import pymysql
from util.mysql_wrapper import MySQLWrapper
from util.parse_crontab import ParseCrontab

class BatchCreateJob():

    def __init__(self,src_source_id, dest_source_id, table_list=[], sync_key="sync_time", queue=None, cron=None, comment=None):

        self.src_source_id = src_source_id
        self.dest_source_id = dest_source_id
        self.table_list = table_list
        self.sync_key = sync_key
        self.queue = queue
        self.cron = cron
        self.comment = comment
        self.mysql_connect = MySQLWrapper()

    # 根据source_id从数据库中获取数据源的连接信息
    def get_source_conn(self, source_id):
        self.mysql_connect.connect()
        sql = f"select source_url,source_port,source_database,source_user,source_passwd from arsenal_data_source where source_id='{source_id}'"
        source_data = self.mysql_connect.fetch_data(sql)
        source_info = {}

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
        
    # 创建任务
    def main(self):

        src_source_conn = self.get_source_conn(self.src_source_id)
        dest_source_conn = self.get_source_conn(self.dest_source_id)
        job_info_list = []

        if not src_source_conn:
            print(f"获取来源数据源[{self.src_source_id}]的MySQL连接失败!")
            return False
        elif not dest_source_conn:
            print(f"获取目标数据源[{self.dest_source_id}]的MySQL连接失败!")
            return False

        if len(self.table_list) == 0:
            with src_source_conn.cursor() as cursor:
                cursor.execute(f"show tables;")
                result=cursor.fetchall()
                if len(result) == 0:
                    print(f"数据源[{self.src_source_id}]中没有表存在!")
                    return False
            for t in result:
                self.table_list.append(t[0])

        # 检查定时任务格式是否正确
        crontab_check_result = ParseCrontab(self.cron).calculate_execution(dry_run=1)
        if not crontab_check_result:
            print(f"crontab[{self.cron}]格式错误")
            return False

        for table_name in self.table_list:
            sql = f'select COLUMN_NAME from information_schema.COLUMNS where TABLE_NAME = "{table_name}";'
            try:
                with src_source_conn.cursor() as cursor:
                    cursor.execute(sql)
                    result=cursor.fetchall()
            except Exception as e:
                print(f"表名为[{table_name}]获取列名失败! error: [{e}]")
                continue

            column_list = []
            for column in result:
                column_list.append(column[0])
            if len(column_list) == 0:
                print(f"表[{table_name}]不存在!")
                continue
            else:
                if self.sync_key not in column_list:
                    print(f"表[{table_name}]中没有同步字段[{self.sync_key}], 跳过此表配置!")
                    continue
 
            job_info = {}
            job_info["src_source_id"]=self.src_source_id
            job_info["src_table"]=table_name
            job_info["dest_source_id"]=self.dest_source_id
            job_info["dest_table"]=table_name
            job_info["sync_key"]=self.sync_key
            job_info["status"]=0
            job_info["queue"]=self.queue
            job_info["cron"]=self.cron
            job_info["comment"]=self.comment
            job_info_list.append(job_info)

        self.mysql_connect.connect()
        self.mysql_connect.insert_multiple_rows("arsenal_sync_job", job_info_list)


if __name__ == "__main__":

    # 样例
    a = BatchCreateJob('src_source_id', 'dest_source_id', table_list=[], sync_key="sync_time", queue="test-queue", cron="* * * * *", comment="任务备注").main()
