# -*- coding: utf-8 -*-
import os
import platform
import configparser
from celery import Celery
from redis import Redis
from contextlib import contextmanager
from util.mysql_wrapper import MySQLWrapper
from util.parse_crontab import ParseCrontab
from arsenal_datax_worker import ArsenalDataxWorker

app = Celery("arsenal_celery")
app.config_from_object('config.celery_config')

# 存储任务锁，跟celery使用相同的即可
conf='default'
cf = configparser.ConfigParser()
workdir = os.getcwd()
if platform.system() == 'Windows':
    ini_path=f"{workdir}\\config\\redis.ini"
elif platform.system() == 'Linux':
    ini_path=f"{workdir}/config/redis.ini"
cf.read(ini_path)
host = cf.get(conf, 'host')
port = int(cf.get(conf, 'port'))
password = cf.get(conf, 'password')
db = int(cf.get(conf, 'db'))

redis_client = Redis(host=host, port=port, password=password, db=db)

# 增加任务锁,增加过期时间和运行状态，防止中断退出导致锁没释放任务无法运行的情况
# 超时时间设置为7天，任务运行超过7天为不正常状态
@contextmanager
def get_lock(job_id):
    lock_key=f"run_data_sync:{job_id}"
    status_result = redis_client.get(lock_key)
    # key存在
    if status_result:
        job_status=int(status_result)
        print(f"任务锁状态为: {job_status}")

        if job_status == 1:
            print(f"任务[{job_id}]处于运行中!")
            last_running_status=True
        else:
            print(f"任务[{job_id}]上次运行成功,运行本次任务!")
            redis_client.set(lock_key, 1, ex=60*60*24*7)
            last_running_status=False
    #key不存在
    else:
        print(f"任务[{job_id}]上次异常退出,或者超时运行, 导致锁过期, 重新设置锁, 并运行本次任务!")
        redis_client.set(lock_key, 1, nx=True, ex=60*60*24*7)
        last_running_status=False

    # 将上次运行结果赋予上下文变量
    running_status=last_running_status

    try:
        yield running_status
    finally:
        if not running_status:
            print(f"任务[{job_id}]结束, 将锁置为0!")
            # 防止运行时间超过7天导致锁过期时，无法将key值置为0
            key_status = redis_client.get(lock_key)
            if key_status:
                redis_client.set(lock_key, '0', ex=60*60*24*7)
            else:
                redis_client.set(lock_key, '0', nx=True, ex=60*60*24*7)

@app.task
def run_data_sync(job_id):
    with get_lock(job_id) as running_status:
        if running_status:
            return f"任务[{job_id}]已在运行,跳过本次运行"
        try:
            ArsenalDataxWorker(job_id).main_handle()
        except Exception as e:
            return f"任务[{job_id}]执行失败, 报错信息: {e}"
        return f"任务[{job_id}]执行成功!"

@app.task
def job_watcher():
    sql="select job_id,cron,queue from arsenal_sync_job;"
    mysql_conn=MySQLWrapper()
    mysql_conn.connect()
    result=mysql_conn.fetch_data(sql)
    for job_info in result:
        job_id=job_info["job_id"]
        job_cron=job_info["cron"]
        job_queue=job_info["queue"]
        if job_cron:
            job_exec_flag=ParseCrontab(job_cron).calculate_execution()
        else:
            job_exec_flag=False
            print(f"{job_id}没有指定定时规则,跳过运行")

        if job_exec_flag:
            print(f"触发任务[{job_id}]运行!")
            if job_queue:
                run_data_sync.apply_async(args=(job_id,), queue=job_queue)
            else:
                run_data_sync.apply_async(args=(job_id,))
            print(f"任务[{job_id}]发送到队列[{job_queue}]")
        else:
            print(f"任务[{job_id}]不在运行时间内!")
    return "任务全部触发完成!"
