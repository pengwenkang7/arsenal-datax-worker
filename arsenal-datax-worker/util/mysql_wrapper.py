# -*- coding: utf-8 -*-
import os
import platform
import pymysql
import configparser

class MySQLWrapper:
    def __init__(self, conf='default'):
        cf = configparser.ConfigParser()
        workdir = os.getcwd()
        if platform.system() == 'Windows':
            ini_path=f"{workdir}\\config\\mysql.ini"
        elif platform.system() == 'Linux':
            ini_path=f"{workdir}/config/mysql.ini"
        cf.read(ini_path)
        self.host = cf.get(conf, 'hostname')
        self.database = cf.get(conf, 'database')
        self.user = cf.get(conf, 'username')
        self.password = cf.get(conf, 'password')
        self.port = int(cf.get(conf, 'hostport'))
        self.charset = cf.get(conf, 'charset')

    def connect(self):
        try:
            self.connection = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                charset=self.charset,
                autocommit=True # 设置自动提交事务
            )
            self.cursor = self.connection.cursor(cursor=pymysql.cursors.DictCursor)
            print("Connected to MySQL database")
        except pymysql.Error as e:
            print(f"Error connecting to MySQL database: {e}")
            return False
 
    def disconnect(self):
        if self.connection:
            self.connection.close()
            print("Disconnected from MySQL database")

    # 执行sql
    def execute_query(self, query, values=None):
        try:
            self.cursor.execute(query, values)
            print("Query executed successfully.")
            self.disconnect()
        except pymysql.Error as e:
            print(f"Error executing query: {e}")
            return False

    # 查询数据
    def fetch_data(self, query, values=None):
        try:
            self.cursor.execute(query, values)
            rows = self.cursor.fetchall()
            self.disconnect()
            return rows
        except pymysql.Error as e:
            print(f"Error fetching data: {e}")
            return False
        
    # 插入单条数据
    def insert_data(self, table, data):
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['%s'] * len(data))
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        values = tuple(data.values())
        self.execute_query(query, values)

    # 插入多条数据
    def insert_multiple_rows(self, table, data):
        columns = ', '.join(data[0].keys())
        placeholders = ', '.join(['%s'] * len(data[0]))
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        values = [tuple(row.values()) for row in data]
        self.execute_query(query, values)

    # 插入多条数据
    def insert_multiple_rows(self, table, data):
        columns = ', '.join(data[0].keys())
        placeholders = ', '.join(['%s'] * len(data[0]))
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        values = [tuple(row.values()) for row in data]
        try:
            self.cursor.executemany(query, values)
            self.disconnect()
        except pymysql.Error as e:
            print(f"Error executing query: {e}")
            return False
    
    # 更新数据
    def update_data(self, table, data, condition):
        column_values = ', '.join([f"{key} = %s" for key in data])
        query = f"UPDATE {table} SET {column_values} WHERE {condition}"
        values = tuple(data.values())
        self.execute_query(query, values)

    # 删除数据
    def delete_data(self, table, condition):
        query = f"DELETE FROM {table} WHERE {condition}"
        self.execute_query(query)

    # 查询存在个数
    def check_value_exists(self, table, column, value):
        query = f"SELECT COUNT(*) FROM {table} WHERE {column} = %s"
        result = self.fetch_data(query, (value,))
        count = result[0][0]
        return count

if __name__ == "__main__":

    wrapper = MySQLWrapper()
    wrapper.connect()

    sql = "select * from arsenal_data_source"

    data=wrapper.fetch_data(sql)
    print(data)
