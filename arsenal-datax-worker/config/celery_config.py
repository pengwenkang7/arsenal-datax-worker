from datetime import timedelta
from kombu import Exchange, Queue

timezone = 'Asia/Shanghai'
broker_url = "redis://:test@localhost:6379/0"
result_backend = "redis://:test@localhost:6379/1"

# 整个Celery的每分钟的并发, 如果任务量大可以多开一些来提高速度
task_annotations = {
    'tasks.add': {'rate_limit': '50/m'}
}

task_queues = [
    Queue('job_watcher', Exchange('job_watcher'), routing_key='job_watcher')
]

beat_schedule = {
    'job_watcher': {
        'task': 'arsenal_celery.job_watcher',
        "schedule": timedelta(minutes=1),
        'options': {
            'queue': 'job_watcher',
        },
        'args':()
    },
} 
