#!/bin/bash

if [ -z $WORKER_NAME ];then

    WORKER_NAME=`cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 16 | head -n 1`

fi

if [ -z $CONC ];then

    CONC=4

fi

cd /opt/arsenal-datax-worker/

# 主节点启动监控页面和定时任务
if [ $ROLE == "MASTER" ];then

    nohup celery -A arsenal_celery worker -n arsenal-data-master --beat --queue=job_watcher --concurrency=4 --loglevel=info --logfile=/opt/arsenal-datax-worker/logs/celery.log &
    nohup celery -A arsenal_celery flower --port=5555 &

# 从节点根据队列进行启动,如果没有指定QUEUE变量使用默认队列消费
elif [ $ROLE == "SLAVE" ];then

    if [ ! -z "$QUEUE" ];then

        nohup celery -A arsenal_celery worker -O fair -n $WORKER_NAME --queue=$QUEUE --concurrency=$CONC --loglevel=info --logfile=/opt/arsenal-datax-worker/logs/celery.log &

    else

        nohup celery -A arsenal_celery worker -O fair -n $WORKER_NAME --concurrency=$CONC --loglevel=info --logfile=/opt/arsenal-datax-worker/logs/celery.log &

    fi 

# 如果两个环境变量没有指定, 则默认启动worker消费默认队列
else

    nohup celery -A arsenal_celery worker -O fair -n $WORKER_NAME --concurrency=$CONC --loglevel=info --logfile=/opt/arsenal-datax-worker/logs/celery.log &

fi

sleep infinity
