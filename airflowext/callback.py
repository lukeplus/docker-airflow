# coding:utf-8

import requests
import json
import os

WEBHOOK_URL = "https://oapi.dingtalk.com/robot/send?access_token=986c123032f16a7613f6669d40e8cd8658067f71df1ac4cf6735125c5fd34316"

tpl_fail_msg = """Airflow DAG执行失败

DAG名称:  {dag_id}
运行ID： {run_id}
本次执行时间：{execution_date}
"""

tpl_fail_msg_check = """Airflow DAG一致性校验失败

DAG名称:  {dag_id}
任务名称：{task_id}
message: {msg}
"""

ENV = os.environ.get("ENV", "dev")


def main_failure_handler(info):
    if ENV == "dev":
        print("Alert disabled in develop environment")
        return

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
    pass


def main_retry_handler(info):
    pass


def check_count_fail(info):
    "同步条目检查失败"
    if ENV == "dev":
        print("Alert disabled in develop environment")
        return
    dag_id, task_id = info["task_id"].split("#")
    data = {
       "dag_id": dag_id,
       "task_id": task_id,
       "msg": "条目数不一致，源表%s条，目的表%s条" % (info["src_count"], info["tar_count"]),
    }
    body = tpl_fail_msg_check.format(**data)
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
    # requests.post(WEBHOOK_URL, data=json.dumps(payload), headers=headers)
