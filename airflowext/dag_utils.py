# coding:utf-8
import os
import re

from jinja2 import Template
from datetime import datetime as dte


CUR_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_PATH = os.path.join(CUR_DIR, "dag.py.template")
TARGET_DIR = os.environ.get("AIRFLOW_HOME", "/tmp")

def generate_dag_file(data):
    """
    生成DAG定义py文件
    """
    with open(TEMPLATE_PATH) as f:
        tpl_content = f.read()

    template = Template(tpl_content)
    today = dte.utcnow()

    parent2child = []
    for t in data["tasks"]:
        if not t["pre_task"]:
            continue
        parent2child.append((t["pre_task"], t["name"]))

    tasks = data["tasks"]
    for t in tasks:
        table = t["target"]["table"]
        if table in ["mes_test_record"]:
            t["append_column"] = "create_date"
        else:
            t["append_column"] = "write_date"
        t["target"]["post_sql_list"] = [sql.strip() for sql in t["target"].get("post_sql", "").split(";") if sql.strip()]

    content = template.render(
        append_column=data["append_column"],
        name=data["name"],
        owner="luke",
        start_date="datetime(%s, %s, %s)" % (today.year, today.month, today.day),
        email="junping.luo@aqara.com",
        retries=1,
        interval=data["interval"],
        tasks=tasks,
        parent2child=parent2child,
    )

    target_path = os.path.join(TARGET_DIR, "dags/auto_%s.py" % data["name"])
    with open(target_path, "w+") as f:
        f.write(content)
    return target_path


if __name__ == "__main__":
    data = {
        "interval": "20s",
        "name": "test",
        "state": "禁用",
        "tasks": [
            {
                "name": "同步表1",
                "sync_type": "全量同步",
                "pre_task": "SELECT",
                "source": {
                    "conn_id": "src_conn",
                    "query_sql": ""
                },
                "target": {
                    "columns": ["xxx"],
                    "conn_id": "tar_conn",
                    "table": "test",
                }
            }
        ]
    }
    generate_dag_file(data)

