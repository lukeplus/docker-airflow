# coding:utf-8


def main_failure_handler(*args, **kwargs):
    print("*" * 100)
    print("failuer")
    print(args, kwargs)


def main_success_handler(*args, **kwargs):
    print("*" * 100)
    print("success")
    print(args, kwargs)


def main_retry_handler(*args, **kwargs):
    print("*" * 100)
    print("retry")
    print(args, kwargs)
