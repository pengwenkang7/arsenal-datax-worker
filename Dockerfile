FROM xxxxxxx

WORKDIR /opt/arsenal-datax-worker

RUN mkdir /opt/arsenal-datax-worker/logs

RUN yum install -y java

COPY datax /opt/datax
COPY datax_mysql5_to_mysql8 /opt/datax_mysql5_to_mysql8
COPY datax_mysql8_to_mysql5 /opt/datax_mysql8_to_mysql5
COPY datax_mysql8 /opt/datax_mysql8

COPY arsenal-datax-worker/requirements.txt /opt/arsenal-datax-worker/

RUN pip3 install --upgrade pip -i https://pypi.tuna.tsinghua.edu.cn/simple \
    && pip3 install --no-cache-dir -r /opt/arsenal-datax-worker/requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

COPY arsenal-datax-worker/ /opt/arsenal-datax-worker/

CMD ["/bin/bash", "/opt/arsenal-datax-worker/docker_run.sh"]
