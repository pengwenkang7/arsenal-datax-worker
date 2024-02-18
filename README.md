# arsenal-datax-worker
基于Datax和Celery的数据同步调度工具

## 实现原理 
基础组件是datax和celery, datax用于执行数据传输任务, celery用于进行分布式调度

## 外部依赖
MySQL:5.7 \
Redis:7.0 \
依赖MySQL存储任务和数据源, Redis用于做celery的任务调度中间件

## 使用方式
修改MySQL配置 config/mysql.ini \
修改celery配置 config/celery_config.py \
修改Redis配置 config/redis.ini \
本程序建议集成为Docker镜像, 集成了datax关于mysql的插件,支持5.7和8.0的互相传输,打包后启动即可使用 \
服务分为master和slave节点, master负责调度和监控不运行任务，slave节点根据指定队列运行相应的任务

## Docker打包
需准备基础镜像替换到Dockerfile中,使用的基础镜像中必须带有java1.8 python2.7 python3.8 \
建议使用CentOS7的基础镜像, 源码安装python3.8.10,作为本项目的基础进行打包, 其他操作系统的基础镜像需要自行修改Dockerfile, 满足环境要求即可

## 数据库导入
数据库文件: db/arsenal_datax.sql \
创建库名为arsenal_datax,导入上面的sql文件即可

## Docker启动方式
通过启动容器时增加配置环境变量来区分主从节点

### 主节点启动方式: \
docker run -tid --name {your_container_name} -e ROLE=MASTER -p5555:5555 image_id \

### 从节点启动方式: 
docker run -tid --name {your_container_name} -e ROLE=SLAVE -e QUEUE={job_queue} -e CONC={job_concurrency} -e WORKER_NAME={worker_name} image_id \

### 环境变量含义: 
ROLE 节点角色, 可选MASTER SLAVE两种, 不指定默认为slave节点 \
QUEUE 节点消费的队列, 如果不指定默认为默认队列 \
CONC 节点的并发进程数量, 如果不指定默认为4 \
WORKER_NAME 节点名称, 如不指定则随机16位数字符串
