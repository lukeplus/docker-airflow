# coding:utf-8

import requests
import json

WEBHOOK_URL = "https://oapi.dingtalk.com/robot/send?access_token=986c123032f16a7613f6669d40e8cd8658067f71df1ac4cf6735125c5fd34316"

tpl_fail_msg = """Airflow DAG执行失败
DAG名称:  {dag_id}
run_id： {run_id}
本次执行时间：{execution_date}
"""


def main_failure_handler(info):
    print("*" * 100)
    data = {
        "dag_id": info["dag"].dag_id,
        "run_id": info["run_id"],
        "execution_date": str(info["execution_date"]),
    }

    body = tpl_fail_msg.format(**data)
    payload = {
        "msgtype": 'text',
        "text": {
            "content": body
        },
        "at": {
            "isAtAll": False
        }
    }
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json;charset=utf-8"
    }
    requests.post(WEBHOOK_URL, data=json.dumps(payload), headers=headers)


def main_success_handler(info):
    print("*" * 100)
    print("success")


def main_retry_handler(info):
    print("*" * 100)
    print("retry")
