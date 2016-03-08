from datetime import datetime


def time():
    return datetime.now()


def time_diff(start, end):
    diff = end -start
    return diff.total_seconds()