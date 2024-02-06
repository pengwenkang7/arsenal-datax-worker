# -*- coding: utf-8 -*-
import os,sys
import json
import time
import datetime
import subprocess
import arrow
import pymysql
from util.mysql_wrapper import MySQLWrapper
from util.migrate_table import MigrateTable

class ArsenalDataxWorker():

    def __init__(self):
        self.mysql_connect = MySQLWrapper()

    # 根据源库信息查询该表的所有字段
    def get_columns_by_table_name(self, db_url, db_port, username, password, database, table):
        conn=pymysql.connect(host=db_url, port=int(db_port), user=username, password=password, database=database, charset='utf8')
        sql = f'select COLUMN_NAME from information_schema.COLUMNS where TABLE_NAME = "{table}";'
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                result=cursor.fetchall()
        except Exception as e:
            print(f"表名为[{table}]获取列名失败! error: [{e}]")
            return False

        column_list = []
        for column in result:
            column_list.append(f"`{column[0]}`")
        if len(column_list) == 0:
            print(f"表[{table}]不存在!")
            return False
        else:
            return column_list

    # 根据source_id获取数据库连接信息
    def get_db_info_by_source_id(self, source_id):
        source_info = {}
        sql = f'select * from arsenal_data_source where source_id="{source_id}"'
        self.mysql_connect.connect()
        result=self.mysql_connect.fetch_data(sql)
        if result:
            source_info['source_url']=result[0]['source_url']
            source_info['source_port']=result[0]['source_port']
            source_info['source_database']=result[0]['source_database']
            source_info['db_type']=result[0]['db_type']
            source_info['source_user']=result[0]['source_user']
            source_info['source_passwd']=result[0]['source_passwd']
            return source_info
        else:
            return False

    # 根据job_id获取数据库的相关信息
    def get_job_info(self, job_id):
        job_info = {}
        sql = f'select * from arsenal_sync_job where job_id = {job_id}'
        self.mysql_connect.connect()
        result=self.mysql_connect.fetch_data(sql)

        if result:
            job_status=result[0]['status']
            if job_status != 1:
                print(f"任务[{job_id}]状态为禁用,跳过运行!")
                return False
            src_source_id=result[0]['src_source_id']
            src_table=result[0]['src_table']
            dest_source_id =result[0]['dest_source_id']
            dest_table=result[0]['dest_table']
            sync_column=result[0]['sync_column']
            job_status=result[0]['status']
            job_info['src_source_id']=src_source_id
            job_info['src_table']=src_table
            job_info['dest_source_id']=dest_source_id
            job_info['dest_table']=dest_table
            job_info['sync_key']=result[0]['sync_key']
            job_info['current_sync_value']=result[0]['current_sync_value']
            job_info['is_full_sync']=result[0]['is_full_sync']
            job_info['is_incr_sync']=result[0]['is_incr_sync']
            job_info['write_mode']=result[0]['write_mode']
            job_info['condition']=result[0]['condition']

        else:
            print(f"任务ID[{job_id}]不存在")
            return False

        src_source_info = self.get_db_info_by_source_id(src_source_id)
        dest_source_info = self.get_db_info_by_source_id(dest_source_id)

        if src_source_info:
            job_info['src_url']=src_source_info['source_url']
            job_info['src_port']=src_source_info['source_port']
            job_info['src_database']=src_source_info['source_database']
            job_info['src_dbtype']=src_source_info['db_type']
            job_info['src_user']=src_source_info['source_user']
            job_info['src_passwd']=src_source_info['source_passwd']
        else:
            print(f"获取源数据源[{src_source_id}]信息出错!")
            return False
        
        if dest_source_info:
            job_info['dest_url']=dest_source_info['source_url']
            job_info['dest_port']=dest_source_info['source_port']
            job_info['dest_database']=dest_source_info['source_database']
            job_info['dest_dbtype']=dest_source_info['db_type']
            job_info['dest_user']=dest_source_info['source_user']
            job_info['dest_passwd']=dest_source_info['source_passwd']
        else:
            print(f"获取目的数据源[{dest_source_id}]信息出错!")
            return False

        src_cloumn=self.get_columns_by_table_name(src_source_info['source_url'], src_source_info['source_port'], src_source_info['source_user'], src_source_info['source_passwd'], job_info['src_database'], src_table)
        dest_cloumn=self.get_columns_by_table_name(dest_source_info['source_url'], dest_source_info['source_port'], dest_source_info['source_user'], dest_source_info['source_passwd'], job_info['dest_database'], dest_table)
        sync_column_list=[]

        # 如果指定列则判断每列在两侧数据源中是否存在,否则以源表全部列为准
        if sync_column:
            column_list=sync_column.split(",")
            for col in column_list:
                col='`'+col+'`'
                if col not in src_cloumn and col not in dest_cloumn:
                    print(f"列[{col}]在源表和目标表有差异,请对比后再重新同步!")
                    return False
                else:
                    sync_column_list.append(col)
        elif src_cloumn and dest_cloumn:
            for col in src_cloumn:
                if col not in dest_cloumn:
                    print(f"列[{col}]在目的表[{dest_table}]不匹配，同步中止!")
                    return False
                else:
                    pass
            sync_column_list=src_cloumn
        else:
            return False
        job_info['sync_column_list']=sync_column_list
        
        return job_info

    # 生成datax的json文件
    def create_datax_json(self, job_id):
        job_info=self.get_job_info(job_id)
        if not job_info:
            print(f"获取任务[{job_id}]信息出错,创建json文件失败,同步中止!")
            return False
        # 源库配置
        src_jdbc_url=f"jdbc:mysql://{job_info['src_url']}:{job_info['src_port']}/{job_info['src_database']}?characterEncoding=UTF-8&useSSL=false" 
        src_table=job_info['src_table']
        src_username=job_info['src_user']
        src_password=job_info['src_passwd']
        # 目标库配置
        dest_jdbc_url=f"jdbc:mysql://{job_info['src_url']}:{job_info['dest_port']}/{job_info['dest_database']}?characterEncoding=UTF-8&useSSL=false"
        dest_table=job_info['dest_table']
        dest_username=job_info['dest_user']
        dest_password=job_info['dest_passwd']
        # 同步配置
        sync_column_list=job_info['sync_column_list']
        sync_key=job_info['sync_key']
        current_sync_value=job_info['current_sync_value']
        is_full_sync=job_info['is_full_sync']
        is_incr_sync=job_info['is_incr_sync']
        write_mode=job_info['write_mode']
        condition=job_info['condition']

        json_body = {}
        json_body['job'] = {"content" : [{ "reader" : {},
                                           "writer" : {} 
                                        }],
                            "setting" : {}}
        # 源库配置
        json_body['job']['content'][0]['reader'] = {"name" : "mysqlreader",
                                                    "parameter" : {}}
        # 编码设置
        json_body['job']['content'][0]['reader']['parameter']['encoding'] = "UTF-8"
        # 连接地址设置
        json_body['job']['content'][0]['reader']['parameter']['connection'] = [{}]
        json_body['job']['content'][0]['reader']['parameter']['connection'][0]['jdbcUrl'] = [src_jdbc_url]
        # 同步配置,在此判断同步配置是否符合要求,不符合就退出
        # full_sync为1表示全量开启，为0表示已经全量关闭
        # increment_sync为0表示增量关闭，为1表示增量同步开启
        if is_full_sync == 1 and is_incr_sync == 0:
            if condition:
                query_sql = f'select * from `{src_table}` where {condition}' 
                json_body['job']['content'][0]['reader']['parameter']['connection'][0]['querySql'] = [query_sql] 
            json_body['job']['content'][0]['reader']['parameter']['connection'][0]['table'] = [f'`{src_table}`']
            json_body['job']['content'][0]['reader']['parameter']['column'] = sync_column_list
        elif is_full_sync == 0 and is_incr_sync == 1 and current_sync_value != 0:
            if condition:
                query_sql = f'select * from `{src_table}` where `{sync_key}` > "{current_sync_value}" and {condition}'
            else:
                query_sql = f'select * from `{src_table}` where `{sync_key}` > "{current_sync_value}"' 
            json_body['job']['content'][0]['reader']['parameter']['connection'][0]['querySql'] = [query_sql]
            json_body['job']['content'][0]['reader']['parameter']['column'] = sync_column_list
        else:
           print(f"任务配置不符合要求,启动失败,请检查!全量同步开关状态为:[{is_full_sync}] 增量同步开关状态为:[{is_incr_sync}] 当前同步值为:[{current_sync_value}]")
           return False

        # 账号密码设置
        json_body['job']['content'][0]['reader']['parameter']['username'] = src_username
        json_body['job']['content'][0]['reader']['parameter']['password'] = src_password
        # 目标库配置
        json_body['job']['content'][0]['writer'] = {"name" : "mysqlwriter",
                                                    "parameter" : {}}
        # 编码格式
        json_body['job']['content'][0]['writer']['parameter']['encoding'] = "UTF-8"
        json_body['job']['content'][0]['writer']['parameter']['column'] = sync_column_list
        # 连接地址设置
        json_body['job']['content'][0]['writer']['parameter']['connection'] = [{}]
        json_body['job']['content'][0]['writer']['parameter']['connection'][0]['jdbcUrl'] = dest_jdbc_url
        json_body['job']['content'][0]['writer']['parameter']['connection'][0]['table'] = [f'`{dest_table}`']
        # 预执行语句,全量之前清空目标表，增量不做处理
        if is_full_sync == 1:
            json_body['job']['content'][0]['writer']['parameter']['preSql'] = [f'truncate table `{dest_table}`']
        else:
            json_body['job']['content'][0]['writer']['parameter']['preSql'] = []
        json_body['job']['content'][0]['writer']['parameter']['session'] = []
        # 写入模式
        if write_mode == 1:
            json_body['job']['content'][0]['writer']['parameter']['writeMode'] = "insert"
        elif write_mode == 2:
            json_body['job']['content'][0]['writer']['parameter']['writeMode'] = "replace"
        else:
            json_body['job']['content'][0]['writer']['parameter']['writeMode'] = "update"
        # 账号密码设置
        json_body['job']['content'][0]['writer']['parameter']['username'] = dest_username
        json_body['job']['content'][0]['writer']['parameter']['password'] = dest_password
        # 传输配置
        json_body['job']['setting']['speed'] = {}
        json_body['job']['setting']['speed']['channel'] = "3"

        json_result = json.dumps(json_body, indent=2)
        return json_result

    # 全量和增量的处理逻辑
    def main_handle(self, job_id):
        job_info=self.get_job_info(job_id)
        if not job_info:
            return False

        src_source_id=job_info['src_source_id']
        src_url=job_info['src_url']
        src_port=job_info['src_port']
        src_db=job_info['src_database']
        src_table=job_info['src_table']
        src_username=job_info['src_user']
        src_password=job_info['src_passwd']
        src_dbtype=job_info['src_dbtype']

        dest_source_id=job_info['dest_source_id']
        dest_table=job_info['dest_table']
        dest_dbtype=job_info['dest_dbtype']

        is_full_sync=job_info['is_full_sync']
        is_incr_sync=job_info['is_incr_sync']
        sync_key=job_info['sync_key']
        current_sync_value=job_info['current_sync_value']

        # 预置检查,检查目的表是否存在, 只处理同名表
        check_table_list = []
        check_table_list.append(src_table)
        check_result=MigrateTable(src_source_id, dest_source_id, check_table_list).migrate_table()
        if check_result:
            print(f"任务[{job_id}]预置检查通过!")
        else:
            print(f"任务[{job_id}]预置检查未通过!")
            return False

        # 查目前的最大值,在全量同步或者增量结束后更新到数据库中,如果最大值为空,则表示表为空表
        try:
            conn = pymysql.connect(host=src_url, port=int(src_port), user=src_username, password=src_password, database=src_db, charset='utf8') 
            get_max_value_sql = f"select max({sync_key}) from `{src_table}`;"
            with conn.cursor() as cursor:
                cursor.execute(get_max_value_sql)
                max_value = cursor.fetchall()[0][0]
                if max_value == None:
                    print(f"库名为[{src_db}]中表名为[{src_table}]自增列最大取值为空，此表为空表，跳过此次备份!")
                    return False
        except Exception as e:
            print(f"表[{src_table}]通过获取同步字段[{sync_key}]获取当前最大值失败! error: [{e}]")
            return False

        print(f"库名为[{src_db}]中表名为[{src_table}]符合备份条件，开始备份!")

        # 首次同步同步值默认为0，跳过比较
        if current_sync_value != '0':
            # 校验自增主键是整数还是时间格式,并对存储的同步值和源库最大主键进行比较判断
            if isinstance(max_value, datetime.datetime):
                max_value_timestamp = int(arrow.get(max_value).timestamp())
                current_sync_value_timestamp = int(arrow.get(current_sync_value).timestamp())
            elif isinstance(max_value, int):
                current_sync_value = int(current_sync_value)
            else:
                print(f"库[{src_db}]中表[{src_table}]的[{sync_key}]同步字段类型或格式错误，只支持整数或时间类型!")
                return False
            if max_value_timestamp == current_sync_value_timestamp:
                print(f"无新增数据,当前同步值为[{str(current_sync_value)}],源表最大值为[{str(max_value)}]")
                return False

        # 检查json和log的存放路径
        day_timestamp=arrow.now().format("YYYYMMDD")
        json_dir = f"/data/arsenal_datax/json/{day_timestamp}"
        log_dir = f"/data/arsenal_datax/log/{day_timestamp}"

        if not os.path.exists(json_dir) or not os.path.exists(log_dir):
            os.makedirs(json_dir)
            os.makedirs(log_dir)

        # 第一次全量更新成功后,full_sync将置为1,increment_sync将置为1,默认后续进行增量更新
        if is_full_sync == 1 and is_incr_sync == 0:
            timestamp = arrow.now().format("YYYYMMDDHHmmss")
            json_name = f"{job_id}-full-sync-{timestamp}.json"
            json_path = f"{json_dir}/{json_name}"
            log_name = f"{job_id}-full-sync-{timestamp}.log"
            log_path = f"{log_dir}/{log_name}"

            with open(json_path, "w") as f:
                json_data=self.create_datax_json(job_id)
                if json_data:
                    f.write(json_data)
            try:
                if src_dbtype == "mysql5" and dest_dbtype == "mysql5":
                    exit_code = subprocess.call(f"python2 /opt/datax/bin/datax.py {json_path} > {log_path} 2>&1", shell=True)
                elif src_dbtype == "mysql5" and dest_dbtype == "mysql8":
                    exit_code = subprocess.call(f"python2 /opt/datax_mysql5_to_mysql8/bin/datax.py {json_path} > {log_path} 2>&1", shell=True)
                elif src_dbtype == "mysql8" and dest_dbtype == "mysql5":
                    exit_code = subprocess.call(f"python2 /opt/datax_mysql8_to_mysql5/bin/datax.py {json_path} > {log_path} 2>&1", shell=True)
                elif src_dbtype == "mysql8" and dest_dbtype == "mysql8":
                    exit_code = subprocess.call(f"python2 /opt/datax_mysql8/bin/datax.py {json_path} > {log_path} 2>&1", shell=True)
                else:
                    print(f"数据库类型错误, 只支持mysql5.7和mysql8.0, 源数据库为[{src_dbtype}], 目标数据库为[{dest_dbtype}]")
                    return False

                # 第一次全量成功之后关闭全量，开启增量
                if exit_code == 0:
                    update_sync_info_sql = f'update arsenal_sync_job set is_full_sync = 0, is_incr_sync = 1, current_sync_value="{max_value}" where job_id={job_id}'
                    self.mysql_connect.connect()
                    self.mysql_connect.execute_query(update_sync_info_sql)
                else:
                    print(f"任务[{job_id}]数据同步失败，请查看本次同步日志{log_path}")
                    return False
            except Exception as e:
                print(f"任务[{job_id}]同步失败! error: [{e}]")
                return False

        # 增量同步
        elif is_full_sync == 0 and is_incr_sync == 1:
            timestamp = arrow.now().format("YYYYMMDDHHmmss")
            json_name = f"{job_id}-incr-sync-{timestamp}.json"
            json_path = f"{json_dir}/{json_name}"
            log_name = f"{job_id}-incr-sync-{timestamp}.log"
            log_path = f"{log_dir}/{log_name}"

            with open(json_path, "w") as f:
                json_data=self.create_datax_json(job_id)
                if json_data:
                    f.write(json_data)
            try:
                if src_dbtype == "mysql5" and dest_dbtype == "mysql5":
                    exit_code = subprocess.call(f"python2 /opt/datax/bin/datax.py {json_path} > {log_path} 2>&1", shell=True)
                elif src_dbtype == "mysql5" and dest_dbtype == "mysql8":
                    exit_code = subprocess.call(f"python2 /opt/datax_mysql5_to_mysql8/bin/datax.py {json_path} > {log_path} 2>&1", shell=True)
                elif src_dbtype == "mysql8" and dest_dbtype == "mysql5":
                    exit_code = subprocess.call(f"python2 /opt/datax_mysql8_to_mysql5/bin/datax.py {json_path} > {log_path} 2>&1", shell=True)
                elif src_dbtype == "mysql8" and dest_dbtype == "mysql8":
                    exit_code = subprocess.call(f"python2 /opt/datax_mysql8/bin/datax.py {json_path} > {log_path} 2>&1", shell=True)
                else:
                    print(f"数据库类型错误, 只支持mysql5.7和mysql8.0, 源数据库为[{src_dbtype}], 目标数据库为[{dest_dbtype}]")
                    return False
                
                if exit_code == 0:
                    update_max_value_sql = f'update arsenal_sync_job set current_sync_value="{max_value}" where job_id = {job_id}'
                    self.mysql_connect.connect()
                    self.mysql_connect.execute_query(update_max_value_sql)
                else:
                    print(f"任务[{job_id}]数据同步失败，请查看本次同步日志{log_path}")
                    return False
            except Exception as e:
                print(f"任务[{job_id}]同步失败! error: [{e}]")
                return False
                
        elif is_full_sync == 0 and is_incr_sync == 0:
            print("已全量同步, 增量开关未打开! ")
            return False
            
        else:
            print("目前只支持第一次全量更新和后续的增量更新, full_sync和incr_sync为[(1,0)全量不增量],[(0,1)增量],[(0,0)不同步]的组合，[(1,1)全量和增量无法都开启]!")
            return False

if __name__ == "__main__":

    job_id = sys.argv[1]
    ArsenalDataxWorker().main_handle(job_id)

