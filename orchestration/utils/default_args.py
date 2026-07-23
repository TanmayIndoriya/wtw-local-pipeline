from datetime import timedelta

default_args = {
    "owner": "me",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}