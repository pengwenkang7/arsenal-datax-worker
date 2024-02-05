from datetime import timedelta
from kombu import Exchange, Queue

timezone = 'Asia/Shanghai'
broker_url = "redis://:test@localhost:6379/0"
result_backend = "redis://:test@localhost:6379/1"

task_annotations = {
    'tasks.add': {'rate_limit': '10/m'}
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
