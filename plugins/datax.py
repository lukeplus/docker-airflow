# -*- coding:utf-8 -*-
# This is the class you derive to create a plugin

import json
import uuid
import subprocess
import os
import json
import re
import time
import pytz

from tempfile import NamedTemporaryFile
from datetime import timedelta, datetime, date
from collections import OrderedDict
from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint, request, redirect
from flask.views import MethodView
from flask_admin import expose
from flask_admin.base import MenuLink
from flask.json import jsonify
from flask import send_file

# Importing base classes that we need to derive
from airflow.utils.state import State
from sqlalchemy import Column, Integer, String, ForeignKey, func
from airflow.models.base import ID_LEN, Base
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.models.baseoperator import BaseOperatorLink
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.executors.base_executor import BaseExecutor
from airflow.utils.decorators import apply_defaults
from airflow.utils import timezone
from flask_appbuilder import BaseView as AppBuilderBaseView
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, Connection, DagModel, DagRun
from airflow.utils.db import create_session, provide_session
from airflow.www_rbac.app import csrf
from airflowext.dag_utils import generate_dag_file
from airflowext.sqlalchemy_utils import dbutil, create_external_session
from airflowext.datax_util import DataXConnectionInfo, RDMS2RDMSDataXJob
from croniter import croniter


SYNC_TYPES = ["增量同步", "全量同步"]


def now_date(fmt):
    return datetime.now(pytz.timezone('Asia/Shanghai')).strftime(fmt)


def valid_cron_expression(exp):
    """
    验证cron表达式是否有效
    """
    return croniter.is_valid(exp)


def timedelta_to_cron_expression(obj):
    """
    timedelta对象转成cron字符串
    """
    if not isinstance(obj, timedelta):
        return obj

    seconds = max(60, int(obj.total_seconds()))

    if seconds < 60 * 60:
        minutes = int(seconds / 60)
        exp = "0/%s * * * *" % minutes
    elif seconds < 60 * 60 * 24:
        hours = int(seconds / (60 * 60))
        exp = "0 0/%s * * *" % hours
    else:
        days = int(seconds / (60 * 60 * 24))
        exp = "0 0 0/%s * *" % days
    return exp


class SyncDAGModel(DagModel):
    __tablename__ = 'sync_dag'
    __mapper_args__ = {'polymorphic_identity': 'sync_dag'}
    sync_dag_id = Column(String(ID_LEN),
                         ForeignKey('dag.dag_id'), primary_key=True)
    sync_type = Column(String(50))  # 已废除
    task_json_str = Column(String(50000), default="[]")

    def to_json(self):
        append_column = "write_date"
        return {
            "name": self.dag_id,
            "interval": timedelta_to_cron_expression(self.schedule_interval),
            "state": self.state,
            "tasks": json.loads(self.task_json_str),
            "append_column": append_column,
        }

    @property
    def tasks(self):
        return json.loads(self.task_json_str)

    @property
    def state(self):
        return "启用" if self.is_active else "禁用"

    @provide_session
    def refresh_dag_file(self, session=None):
        path = generate_dag_file(self.to_json())
        self.fileloc = path
        session.commit()

    def delete_dag_file(self):
        if os.path.exists(self.fileloc):
            os.unlink(self.fileloc)

    def dumps(self):
        tasks = []
        for t in json.loads(self.task_json_str):
            tasks.append(OrderedDict({
                "name": t["name"],
                "parent": t["pre_task"],
                "sync_type": t["sync_type"],
                "append_basis": t["append_basis"],
                "source": {
                    "conn_id": t["source"]["conn_id"],
                    "query_sql": t["source"]["query_sql"],
                    "source_from": t["source"].get("source_from", "")
                },
                "target": {
                    "conn_id": t["target"]["conn_id"],
                    "table": t["target"]["table"],
                    "columns": t["target"]["columns"],
                    "source_from_column": t["target"].get("source_from_column", ""),
                    "primary_key": t["target"].get("pkeys", ""),
                    "post_sql": t["target"].get("post_sql", "")
                },
            }))

        data = OrderedDict({
            "name": self.dag_id,
            "interval": self.schedule_interval,
            "tasks": tasks
        })
        return json.dumps(data, ensure_ascii=False, indent=2)

    @classmethod
    def load(cls, raw):
        tasks = []
        for t in raw["tasks"]:
            tasks.append(OrderedDict({
                "name": t["name"],
                "pre_task": t["parent"],
                "sync_type": t["sync_type"],
                "append_basis": t["append_basis"],
                "source": {
                    "conn_id": t["source"]["conn_id"],
                    "query_sql": t["source"]["query_sql"],
                    "source_from": t["source"].get("source_from", "")
                },
                "target": {
                    "conn_id": t["target"]["conn_id"],
                    "table": t["target"]["table"],
                    "columns": t["target"]["columns"],
                    "source_from_column": t["target"].get("source_from_column", ""),
                    "pkeys": t["target"].get("primary_key", ""),
                    "post_sql": t["target"].get("post_sql", "")
                },
            }))

        data = OrderedDict({
            "name": raw["name"],
            "interval": raw["interval"],
            "tasks": tasks
        })
        return data


class RDMS2RDMSOperator(BaseOperator):
    template_fields = ('src_query_sql',  'tar_table', 'tar_columns')
    ui_color = '#edd5f1'

    @apply_defaults
    def __init__(self,
                 sync_type,
                 append_basis,
                 src_conn_id,
                 src_query_sql,
                 src_source_from,
                 tar_conn_id,
                 tar_table,
                 tar_columns,
                 append_column,
                 is_link_table,
                 tar_pkeys,
                 tar_source_from_column,
                 tar_post_sql_list,
                 *args,
                 **kwargs):
        """
            :param append_column: 增量字段; 当同步类型为增量同步时有效。
        """

        super().__init__(*args, **kwargs)
        assert sync_type in SYNC_TYPES
        self.sync_type = sync_type
        self.src_conn_id = src_conn_id
        self.src_query_sql = src_query_sql
        self.src_source_from = src_source_from
        self.tar_conn_id = tar_conn_id
        self.tar_table = tar_table
        self.tar_columns = tar_columns
        self.append_column = append_column
        self.append_basis = append_basis or "目的库时间"
        self.tar_pkeys = [c.strip() for c in tar_pkeys.split(",") if c.strip()]
        if not self.tar_pkeys:
            self.tar_pkeys = ["id"]
        self.tar_source_from_column = tar_source_from_column
        self.tar_post_sql_list = tar_post_sql_list
        self.is_link_table = is_link_table or "否"

    def execute(self, context):
        """
        Execute
        """
        self.log.info('RDMS2RDMSOperator execute...')
        task_id = context['task_instance'].dag_id + "#" + context['task_instance'].task_id

        if self.sync_type == "全量同步":
            if self.is_link_table == "是":
                self.hook = RDBMS2RDBMSFullHook2(
                                task_id=task_id,
                                src_conn_id=self.src_conn_id,
                                src_query_sql=self.src_query_sql,
                                src_source_from=self.src_source_from,
                                tar_conn_id=self.tar_conn_id,
                                tar_table=self.tar_table,
                                tar_columns=self.tar_columns,
                                tar_pkeys=self.tar_pkeys,
                                tar_source_from_column=self.tar_source_from_column,
                                tar_post_sql_list=self.tar_post_sql_list
                            )
            else:
                self.hook = RDBMS2RDBMSFullHook(
                                task_id=task_id,
                                src_conn_id=self.src_conn_id,
                                src_query_sql=self.src_query_sql,
                                src_source_from=self.src_source_from,
                                tar_conn_id=self.tar_conn_id,
                                tar_table=self.tar_table,
                                tar_columns=self.tar_columns,
                                tar_pkeys=self.tar_pkeys,
                                tar_source_from_column=self.tar_source_from_column,
                                tar_post_sql_list=self.tar_post_sql_list
                            )
        elif self.sync_type == "增量同步":
            if self.append_basis == "源库时间":
                self.hook = RDBMS2RDBMSAppendHook2(
                                dag=self.dag,
                                task_id=task_id,
                                src_conn_id=self.src_conn_id,
                                src_query_sql=self.src_query_sql,
                                src_source_from=self.src_source_from,
                                tar_conn_id=self.tar_conn_id,
                                tar_table=self.tar_table,
                                tar_columns=self.tar_columns,
                                append_column=self.append_column,
                                tar_pkeys=self.tar_pkeys,
                                tar_source_from_column=self.tar_source_from_column,
                                tar_post_sql_list=self.tar_post_sql_list
                            )
            else:
                self.hook = RDBMS2RDBMSAppendHook(
                                dag=self.dag,
                                task_id=task_id,
                                src_conn_id=self.src_conn_id,
                                src_query_sql=self.src_query_sql,
                                src_source_from=self.src_source_from,
                                tar_conn_id=self.tar_conn_id,
                                tar_table=self.tar_table,
                                tar_columns=self.tar_columns,
                                append_column=self.append_column,
                                tar_pkeys=self.tar_pkeys,
                                tar_source_from_column=self.tar_source_from_column,
                                tar_post_sql_list=self.tar_post_sql_list
                            )
        self.hook.execute(context=context)

    def on_kill(self):
        self.log.info('Sending SIGTERM signal to bash process group')
        os.killpg(os.getpgid(self.hook.sp.pid), signal.SIGTERM)


class RDBMS2RDBMSFullHook(BaseHook):
    """
    Datax执行器:全量同步
    """

    def __init__(self,
                 task_id,
                 src_conn_id,
                 src_query_sql,
                 src_source_from,
                 tar_conn_id,
                 tar_table,
                 tar_columns,
                 tar_pkeys,
                 tar_source_from_column,
                 tar_post_sql_list):
        self.task_id = task_id
        self.src_conn = self.get_connection(src_conn_id)
        self.src_query_sql = src_query_sql
        self.src_source_from = src_source_from
        self.tar_conn = self.get_connection(tar_conn_id)
        self.tar_table = tar_table
        self.tar_columns = tar_columns
        self.tar_pkeys = tar_pkeys
        self.tar_source_from_column = tar_source_from_column
        self.tar_post_sql_list = tar_post_sql_list
        self.init()

    def cal_source_from_value(self):
        lst = self.src_source_from.split(":")
        if len(lst) > 1 and lst[0].lower() == "function":
            return eval(lst[1])
        return self.src_source_from

    def init(self):
        where = ""
        if self.tar_source_from_column:
            where = "%s='%s'" % (self.tar_source_from_column, self.cal_source_from_value())

        sql = "DELETE FROM %s" % self.tar_table
        if where:
            sql = sql + " WHERE " + where
        self.tar_pre_sql = sql
        self.log.info('pre_sql: %s', sql)

    def execute(self, context):
        self.log.info('RDMS2RDMSOperator execute...')

        self.task_id = context['task_instance'].dag_id + "#" + context['task_instance'].task_id
        self.run_datax_job()

    def trans_conn_to_datax_conn(self, conn):
        """
            airflow Connection对象转datax的DataXConnectionInfo对象
        """
        return DataXConnectionInfo(
            conn.conn_type,
            conn.host.strip(),
            str(conn.port),
            conn.schema.strip(),
            conn.login.strip(),
            conn.password.strip(),
        )

    def run_datax_job(self):
        job = RDMS2RDMSDataXJob(self.task_id,
                                self.trans_conn_to_datax_conn(self.src_conn),
                                self.trans_conn_to_datax_conn(self.tar_conn),
                                self.src_query_sql,
                                self.tar_table,
                                self.tar_columns,
                                self.tar_pre_sql,
                                self.tar_post_sql_list)
        job.execute()


class RDBMS2RDBMSFullHook2(BaseHook):
    """
    Datax执行器:全量同步, 只针对关联表的同步
    """

    def __init__(self,
                 task_id,
                 src_conn_id,
                 src_query_sql,
                 src_source_from,
                 tar_conn_id,
                 tar_table,
                 tar_columns,
                 tar_pkeys,
                 tar_source_from_column,
                 tar_post_sql_list):
        self.task_id = task_id
        self.src_conn = self.get_connection(src_conn_id)
        self.src_query_sql = src_query_sql
        self.src_source_from = src_source_from
        self.tar_conn = self.get_connection(tar_conn_id)
        self.tar_table = tar_table
        self.tmp_tar_table = "tmp_append_%s" % tar_table
        if src_source_from:
            self.tmp_tar_table = self.tmp_tar_table + "_" + src_source_from.split(":")[0]
        self.tar_columns = tar_columns
        self.tar_pkeys = tar_pkeys
        self.tar_source_from_column = tar_source_from_column
        self.tar_post_sql_list = tar_post_sql_list
        self.init()

    def init(self):
        where = ""
        if self.tar_source_from_column:
            where = "%s='%s'" % (self.tar_source_from_column, self.cal_source_from_value())

        sql = "DELETE FROM %s" % self.tmp_tar_table
        if where:
            sql = sql + " WHERE " + where
        self.tar_pre_sql = sql
        self.log.info('pre_sql: %s', sql)

    def cal_source_from_value(self):
        lst = self.src_source_from.split(":")
        if len(lst) > 1 and lst[0].lower() == "function":
            return eval(lst[1])
        return self.src_source_from

    def drop_temp_table(self):
        """
        删掉临时表
        """
        drop_sql = "DROP TABLE IF EXISTS {table}"
        if self.tar_conn.conn_type.strip() in ["mssql", "sqlserver"]:
            drop_sql = """
            IF EXISTS
                (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table}')
                DROP TABLE {table}
            """
        drop_sql = drop_sql.format(table=self.tmp_tar_table)
        with create_external_session(self.tar_conn) as sess:
            sess.execute(drop_sql)

    def create_temp_table(self):
        """
        创建用于同步的临时表
        """
        create_sql = "CREATE TABLE {new_table} AS (SELECT * FROM {old_table} WHERE 1=2)"
        if self.tar_conn.conn_type.strip() in ["mssql", "sqlserver"]:
            create_sql = "Select * into {new_table} from {old_table} WHERE 1=2"

        data = {
            "new_table": self.tmp_tar_table,
            "old_table": self.tar_table
        }
        create_sql = create_sql.format(**data)
        with create_external_session(self.tar_conn) as sess:
            sess.execute(create_sql)

    def migrate_temp_table_to_tar_table(self):
        """
        把数据从临时表迁移到正式表
        步骤：
            1. 将删除行同步到正式表
            2. 将插入行同步到正式表

        注意：关联表不存在更新情况
        """
        pkeys_caluse = " AND ".join(["%s.%s=tmp.%s" % (self.tar_table, c, c) for c in self.tar_columns])
        columns = ",".join(['"%s"' % c for c in self.tar_columns])
        data = {
            "table": self.tar_table,
            "temp": self.tmp_tar_table,
            "pkeys_caluse": pkeys_caluse,
            "columns": columns,
        }

        insert_sql = """
            INSERT INTO {table} ({columns})
            (SELECT {columns} FROM {temp} tmp WHERE not exists
            (SELECT 1 FROM {table} WHERE {pkeys_caluse}));
        """
        insert_sql = insert_sql.format(**data)
        delete_sql = """
            DELETE FROM {table} WHERE NOT EXISTS
            (SELECT 1 FROM {temp} tmp WHERE {pkeys_caluse})
        """
        delete_sql = delete_sql.format(**data)
        with create_external_session(self.tar_conn) as sess:
            self.log.info("migrate_temp_table_to_tar_table insert_sql: %s", insert_sql)
            sess.execute(insert_sql)
            self.log.info("migrate_temp_table_to_tar_table delete_sql: %s", delete_sql)
            sess.execute(delete_sql)

    def execute(self, context):
        self.log.info('RDMS2RDMSOperator execute...')

        self.task_id = context['task_instance'].dag_id + "#" + context['task_instance'].task_id

        # self.drop_temp_table()
        # self.create_temp_table()
        self.run_datax_job()
        self.migrate_temp_table_to_tar_table()
        # self.drop_temp_table()

    def trans_conn_to_datax_conn(self, conn):
        """
            airflow Connection对象转datax的DataXConnectionInfo对象
        """
        return DataXConnectionInfo(
            conn.conn_type,
            conn.host.strip(),
            str(conn.port),
            conn.schema.strip(),
            conn.login.strip(),
            conn.password.strip(),
        )

    def run_datax_job(self):
        job = RDMS2RDMSDataXJob(self.task_id,
                                self.trans_conn_to_datax_conn(self.src_conn),
                                self.trans_conn_to_datax_conn(self.tar_conn),
                                self.src_query_sql,
                                self.tmp_tar_table,
                                self.tar_columns,
                                self.tar_pre_sql,
                                self.tar_post_sql_list)
        job.execute()


class RDBMS2RDBMSAppendHook(BaseHook):
    """
    Datax执行器: 增量同步
    """

    def __init__(self,
                 dag,
                 task_id,
                 src_conn_id,
                 src_query_sql,
                 src_source_from,
                 tar_conn_id,
                 tar_table,
                 tar_columns,
                 append_column,
                 tar_pkeys,
                 tar_source_from_column,
                 tar_post_sql_list):
        self.task_id = task_id
        self.dag = dag
        self.src_conn = self.get_connection(src_conn_id)
        self.src_query_sql = src_query_sql
        self.src_source_from = src_source_from
        self.tar_conn = self.get_connection(tar_conn_id)
        self.tar_table = tar_table
        self.tmp_tar_table = "tmp_append_%s" % tar_table
        if src_source_from:
            self.tmp_tar_table = self.tmp_tar_table + "_" + src_source_from.split(":")[0]
        self.tar_columns = tar_columns
        self.append_column = append_column
        self.max_append_column_value = None
        self.tar_pkeys = tar_pkeys
        self.tar_source_from_column = tar_source_from_column
        self.tar_post_sql_list = tar_post_sql_list

    def execute(self, context):
        """
        Execute
        """
        self.task_id = context['task_instance'].dag_id + "#" + context['task_instance'].task_id

        self.drop_temp_table()
        self.create_temp_table()
        self.refresh_max_append_column_value()
        self.run_datax_job()
        self.migrate_temp_table_to_tar_table()
        self.drop_temp_table()

    def refresh_max_append_column_value(self):
        """
        刷新增量字段的最大值
        """
        where = ""
        if self.tar_source_from_column:
            where = "%s='%s'" % (self.tar_source_from_column, self.src_source_from)

        sql = "SELECT max(%s) FROM %s " % (self.append_column, self.tar_table)
        if where:
            sql = sql + " WHERE " + where

        self.log.info("refresh_max_append_column_value sql: %s", sql)
        with create_external_session(self.tar_conn) as sess:
            result = sess.execute(sql)
        record = result.fetchone()
        if record:
            self.max_append_column_value = record[0]
        return self.max_append_column_value

    def create_temp_table(self):
        """
        创建用于增量同步的临时表
        """
        create_sql = "CREATE TABLE {new_table} AS (SELECT * FROM {old_table} WHERE 1=2)"
        if self.tar_conn.conn_type.strip()  in ["mssql", "sqlserver"]:
            create_sql = "Select * into {new_table} from {old_table} WHERE 1=2"

        data = {
            "new_table": self.tmp_tar_table,
            "old_table": self.tar_table
        }
        create_sql = create_sql.format(**data)
        with create_external_session(self.tar_conn) as sess:
            sess.execute(create_sql)

    def drop_temp_table(self):
        """
        删掉临时表
        """
        drop_sql = "DROP TABLE IF EXISTS {table}"
        if self.tar_conn.conn_type.strip()  in ["mssql", "sqlserver"]:
            drop_sql = """
            IF EXISTS
                (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table}')
                DROP TABLE {table}
            """
        drop_sql = drop_sql.format(table=self.tmp_tar_table)
        with create_external_session(self.tar_conn) as sess:
            sess.execute(drop_sql)

    def migrate_temp_table_to_tar_table(self):
        """
        把数据从临时表迁移到正式表
        分为两步：
            1. 把更新的行同步过去
            2. 把新增的行同步过去
        """
        set_caluse = ",".join(["%s=b.%s" % (c, c) for c in self.tar_columns])
        pkeys_caluse = " AND ".join(["a.%s=b.%s" % (c, c) for c in self.tar_pkeys])
        pkeys_caluse2 = " AND ".join(["%s=tmp.%s" % (c, c) for c in self.tar_pkeys])
        columns = ",".join(['"%s"' % c for c in self.tar_columns])
        data = {
            "table": self.tar_table,
            "temp": self.tmp_tar_table,
            "set_caluse": set_caluse,
            "pkeys_caluse": pkeys_caluse,
            "pkeys_caluse2": pkeys_caluse2,
            "columns": columns,
        }

        update_sql = """UPDATE {table} a SET {set_caluse} FROM {temp} b WHERE {pkeys_caluse}"""
        update_sql = update_sql.format(**data)
        insert_sql = """INSERT INTO {table} ({columns}) (SELECT {columns} FROM {temp} tmp WHERE not exists (SELECT 1 FROM {table} WHERE {pkeys_caluse2}));"""
        insert_sql = insert_sql.format(**data)
        with create_external_session(self.tar_conn) as sess:
            self.log.info("migrate_temp_table_to_tar_table start")
            self.log.info("migrate_temp_table_to_tar_table update_sql: %s", update_sql)
            sess.execute(update_sql)
            self.log.info("migrate_temp_table_to_tar_table insert_sql: %s", insert_sql)
            sess.execute(insert_sql)
            self.log.info("migrate_temp_table_to_tar_table end")

    def trans_conn_to_datax_conn(self, conn):
        return DataXConnectionInfo(
            conn.conn_type,
            conn.host.strip(),
            str(conn.port),
            conn.schema.strip(),
            conn.login.strip(),
            conn.password.strip(),
        )

    def generat_new_src_query_sql(self):
        if not self.max_append_column_value:
            return self.src_query_sql
        sql = "SELECT * FROM ({}) as main_ WHERE main_.{} >= '{}'"
        return sql.format(self.src_query_sql,
                          self.append_column,
                          self.max_append_column_value)

    def generate_new_tar_pre_sql(self):
        return ""

    def run_datax_job(self):
        job = RDMS2RDMSDataXJob(self.task_id,
                                self.trans_conn_to_datax_conn(self.src_conn),
                                self.trans_conn_to_datax_conn(self.tar_conn),
                                self.generat_new_src_query_sql(),
                                self.tmp_tar_table,
                                self.tar_columns,
                                self.generate_new_tar_pre_sql(),
                                self.tar_post_sql_list)
        job.execute()


class RDBMS2RDBMSAppendHook2(BaseHook):
    """
    Datax执行器: 增量同步

    跟RDBMS2RDBMSAppendHook的区别是，以源库的时间作为同步基准
    """

    def __init__(self,
                 dag,
                 task_id,
                 src_conn_id,
                 src_query_sql,
                 src_source_from,
                 tar_conn_id,
                 tar_table,
                 tar_columns,
                 append_column,
                 tar_pkeys,
                 tar_source_from_column,
                 tar_post_sql_list):
        self.dag = dag
        self.task_id = task_id
        self.src_conn = self.get_connection(src_conn_id)
        self.src_query_sql = src_query_sql
        self.src_source_from = src_source_from
        self.tar_conn = self.get_connection(tar_conn_id)
        self.tar_table = tar_table
        self.tmp_tar_table = "tmp_append_%s" % tar_table
        if src_source_from:
            self.tmp_tar_table = self.tmp_tar_table + "_" + src_source_from.split(":")[0]
        self.tar_columns = tar_columns
        self.append_column = append_column
        self.max_append_value = None
        self.tar_pkeys = tar_pkeys
        self.tar_source_from_column = tar_source_from_column
        self.tar_post_sql_list = tar_post_sql_list

    def execute(self, context):
        """
        Execute
        """
        self.task_id = context['task_instance'].dag_id + "#" + context['task_instance'].task_id

        self.drop_temp_table()
        self.create_temp_table()
        self.load_max_append_value()
        self.refresh_new_src_query_sql()
        self.run_datax_job()
        self.migrate_temp_table_to_tar_table()
        self.refresh_max_append_value()
        self.drop_temp_table()
        self.save_max_append_value()

    @provide_session
    def load_max_append_value(self, session=None):
        """
        读取同步时间最大值

        若有前置任务，则使用前置任务的增量字段值
        """
        dag = session.query(SyncDAGModel).get(self.dag.dag_id)
        task_name = self.task_id.split("#")[1]

        task_dct = {}
        for t in dag.tasks:
            task_dct[t["name"]] = t

        root_name = task_name
        while task_dct[root_name]["pre_task"]:
            root_name = task_dct[root_name]["pre_task"]

        default = "1997-01-01"
        if root_name == task_name:
            self.max_append_value = task_dct[task_name].get("max_append_value", default)
        else:
            self.max_append_value = task_dct[root_name].get("last_max_append_value", default)
        self.log.info("同步时间最大值：%s" % self.max_append_value)
        return self.max_append_value

    @provide_session
    def save_max_append_value(self, session=None):
        """
        保存本次同步时间
        """

        dag = session.query(SyncDAGModel).get(self.dag.dag_id)
        task_name = self.task_id.split("#")[1]
        tasks = dag.tasks

        task_dct = {}
        for t in tasks:
            task_dct[t["name"]] = t

        root_name = task_name
        while task_dct[root_name]["pre_task"]:
            root_name = task_dct[root_name]["pre_task"]

        default = "1997-01-01"
        if root_name == task_name:
            task_dct[task_name]["last_max_append_value"] = task_dct[task_name].get("max_append_value", default)
            task_dct[task_name]["max_append_value"] = self.max_append_value
        else:
            task_dct[task_name]["last_max_append_value"] = task_dct[root_name].get("max_append_value", default)
            task_dct[task_name]["max_append_value"] = min(task_dct[root_name]["max_append_value"], self.max_append_value)

        dag.task_json_str = json.dumps(tasks)
        session.commit()

    def refresh_max_append_value(self):
        """
        刷新本次同步时间
        """
        where = ""
        if self.tar_source_from_column:
            where = "%s='%s'" % (self.tar_source_from_column, self.src_source_from)

        sql = "SELECT max(%s) FROM %s " % (self.append_column, self.tmp_tar_table)
        if where:
            sql = sql + " WHERE " + where

        with create_external_session(self.tar_conn) as sess:
            result = sess.execute(sql)
        record = result.fetchone()
        if record[0]:
            self.max_append_value = record[0].strftime("%Y-%m-%d %H:%M:%S")
        return self.max_append_value

    def create_temp_table(self):
        """
        创建用于增量同步的临时表
        """
        create_sql = "CREATE TABLE {new_table} AS (SELECT * FROM {old_table} WHERE 1=2)"
        if self.tar_conn.conn_type.strip() in ["mssql", "sqlserver"]:
            create_sql = "Select * into {new_table} from {old_table} WHERE 1=2"

        data = {
            "new_table": self.tmp_tar_table,
            "old_table": self.tar_table
        }
        create_sql = create_sql.format(**data)
        with create_external_session(self.tar_conn) as sess:
            sess.execute(create_sql)

    def drop_temp_table(self):
        """
        删掉临时表
        """
        drop_sql = "DROP TABLE IF EXISTS {table}"
        if self.tar_conn.conn_type.strip()  in ["mssql", "sqlserver"]:
            drop_sql = """
            IF EXISTS
                (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table}')
                DROP TABLE {table}
            """
        drop_sql = drop_sql.format(table=self.tmp_tar_table)
        with create_external_session(self.tar_conn) as sess:
            sess.execute(drop_sql)

    def migrate_temp_table_to_tar_table(self):
        """
        把数据从临时表迁移到正式表
        分为两步：
            1. 把更新的行同步过去
            2. 把新增的行同步过去
        """
        set_caluse = ",".join(["%s=b.%s" % (c, c) for c in self.tar_columns])
        pkeys_caluse = " AND ".join(["a.%s=b.%s" % (c, c) for c in self.tar_pkeys])
        pkeys_caluse2 = " AND ".join(["%s=tmp.%s" % (c, c) for c in self.tar_pkeys])
        columns = ",".join(['"%s"' % c for c in self.tar_columns])
        data = {
            "table": self.tar_table,
            "temp": self.tmp_tar_table,
            "set_caluse": set_caluse,
            "pkeys_caluse": pkeys_caluse,
            "pkeys_caluse2": pkeys_caluse2,
            "columns": columns,
        }

        update_sql = """UPDATE {table} a SET {set_caluse} FROM {temp} b WHERE {pkeys_caluse}"""
        update_sql = update_sql.format(**data)
        insert_sql = """INSERT INTO {table} ({columns}) (SELECT {columns} FROM {temp} tmp WHERE not exists (SELECT 1 FROM {table} WHERE {pkeys_caluse2}));"""
        insert_sql = insert_sql.format(**data)
        with create_external_session(self.tar_conn) as sess:
            self.log.info("migrate_temp_table_to_tar_table update_sql: %s", update_sql)
            t1 = time.time()
            sess.execute(update_sql)
            self.log.info("migrate_temp_table_to_tar_table insert_sql: %s", insert_sql)
            sess.execute(insert_sql)

    def trans_conn_to_datax_conn(self, conn):
        return DataXConnectionInfo(
            conn.conn_type,
            conn.host.strip(),
            str(conn.port),
            conn.schema.strip(),
            conn.login.strip(),
            conn.password.strip(),
        )

    def refresh_new_src_query_sql(self):
        if self.max_append_value:
            sql = "SELECT * FROM ({}) as main_ WHERE main_.{} >= '{}'"
            sql = sql.format(self.src_query_sql,
                             self.append_column,
                             self.max_append_value)
            self.new_src_query_sql = sql
        else:
            self.new_src_query_sql = self.src_query_sql


    def generate_new_tar_pre_sql(self):
        return ""

    def run_datax_job(self):
        job = RDMS2RDMSDataXJob(self.task_id,
                                self.trans_conn_to_datax_conn(self.src_conn),
                                self.trans_conn_to_datax_conn(self.tar_conn),
                                self.new_src_query_sql,
                                self.tmp_tar_table,
                                self.tar_columns,
                                self.generate_new_tar_pre_sql(),
                                self.tar_post_sql_list)
        job.execute()


# Will show up under airflow.sensors.test_plugin.PluginSensorOperator
class PluginSensorOperator(BaseSensorOperator):
    pass


# Will show up under airflow.executors.test_plugin.PluginExecutor
class PluginExecutor(BaseExecutor):
    pass


# Will show up under airflow.macros.test_plugin.plugin_macro
# and in templates through {{ macros.test_plugin.plugin_macro }}
def plugin_macro():
    pass


# Creating a flask blueprint to integrate the templates and static folder
bp = Blueprint(
    "datax", __name__,
    template_folder='templates',    # registers airflow/plugins/templates as a Jinja template folder
    static_folder='static',
    static_url_path='/static/datax')

csrf.exempt(bp)


class SyncDAGListView(MethodView):

    @provide_session
    def post(self, session=None):
        """
        增加syncdag

        Input:
            {
                "name": "xx",
                "sync_type": "增量同步",
                "interval": "10s",
                "tasks": [{
                    "name": "yy",
                    "pre_task": "zz",
                    "source":{
                        "conn_id": "",
                        "query_sql": "",
                    },
                    "target":{
                        "conn_id": "",
                        "columns": [""],
                    }
                }]
            }
        """
        params = json.loads(request.data)
        name = params["name"]

        dag = session.query(DagModel).filter_by(dag_id=name).first()
        if dag:
            return jsonify({
                "code": -1,
                "msg": "名字为%s的DAG已存在!" % name
            })

        if not valid_cron_expression(params["interval"]):
            return jsonify({
                "code": -1,
                "msg": "非法的cron表达式"
            })

        dag = SyncDAGModel(
            dag_id=name,
            owners="luke",
            schedule_interval=params["interval"],
            fileloc="",
            task_json_str=json.dumps(params["tasks"]),
            is_active=True,
            is_paused=False,
        )
        session.add(dag)
        session.commit()

        dag.refresh_dag_file()
        return jsonify({
            "code": 0,
            "msg": "新建成功"
        })

    @provide_session
    def get(self, session=None):
        """
        获取DAG
        """
        dags = session.query(SyncDAGModel).all()
        lst = [dag.to_json() for dag in dags]
        return jsonify(lst)


class SyncDAGDetailView(MethodView):

    @provide_session
    def delete(self, dag_id, session=None):
        """
        删除DAG
        """
        dag = session.query(SyncDAGModel).get(dag_id)
        if not dag:
            return jsonify({
                "code": -1,
                "msg": "不存在名为%s的dag" % dag_id
            })
        session.delete(dag)
        session.commit()
        dag.delete_dag_file()

        return jsonify({
            "code": 0,
            "msg": "删除成功"
        })

    @provide_session
    def put(self, dag_id, session=None):
        """
        修改DAG
        """
        dag = session.query(SyncDAGModel).get(dag_id)
        if not dag:
            return jsonify({
                "code": -1,
                "msg": "不存在名为%s的dag" % dag_id
            })

        params = json.loads(request.data)
        if not valid_cron_expression(params["interval"]):
            return jsonify({
                "code": -1,
                "msg": "非法的cron表达式"
            })

        dag.schedule_interval = params["interval"]
        dag.task_json_str = json.dumps(params["tasks"])
        session.commit()
        dag.refresh_dag_file()

        return jsonify({
            "code": 0,
            "msg": "修改成功"
        })

    @provide_session
    def get(self, dag_id, session=None):
        """
        获取DAG
        """
        dag = session.query(SyncDAGModel).get(dag_id)
        if not dag:
            return jsonify({
                "code": -1,
                "msg": "不存在名为%s的dag" % dag_id
            })
        return jsonify(dag.to_json())


@bp.route("/datax/api/connections", methods=["GET"])
@provide_session
@csrf.exempt
def get_connections(session=None):
    conns = session.query(Connection).all()
    conn_ids = [c.conn_id for c in conns]
    return jsonify({
        "code": 0,
        "msg": "OK",
        "connections": conn_ids,
    })


@bp.route("/datax/api/connection/<conn_id>/tables", methods=["GET"])
@csrf.exempt
@provide_session
def get_tables(conn_id, session=None):
    conn = session.query(Connection).filter_by(conn_id=conn_id).first()
    if not conn:
        return jsonify({
            "code": -1,
            "msg": "不存在名为%s的Connection" % conn_id,
        })

    with create_external_session(conn) as external_session:
        tables = dbutil.get_tables(external_session)
    return jsonify({
        "code": 0,
        "msg": "SUCCESS",
        "tables": tables
    })


@bp.route("/datax/api/connection/<conn_id>/table/<table_name>/columns", methods=["GET"])
@csrf.exempt
@provide_session
def get_columns(conn_id, table_name, session=None):
    conn = session.query(Connection).filter_by(conn_id=conn_id).first()
    if not conn:
        return jsonify({
            "code": -1,
            "msg": "不存在名为%s的Connection" % conn_id,
        })
    with create_external_session(conn) as external_session:
        columns = dbutil.get_cloumns(external_session, table_name)
    return jsonify({
        "code": 0,
        "msg": "SUCCESS",
        "tables": columns
    })

@bp.route("/datax/api/dag/import", methods=["POST"])
@provide_session
@csrf.exempt
def import_dag_from_file(session=None):
    "从文件导入dag"
    file = request.files['file']
    try:
        data = json.loads(file.read())
    except Exception:
        return jsonify({
            "code": -1,
            "msg": "invalid json format",
        })
    data = SyncDAGModel.load(data)

    name = data["name"]
    dag = session.query(DagModel).filter_by(dag_id=name).first()
    if dag:
        return jsonify({
            "code": -1,
            "msg": "名字为%s的DAG已存在!" % name
        })

    if not valid_cron_expression(data["interval"]):
        return jsonify({
            "code": -1,
            "msg": "非法的cron表达式"
        })

    dag = SyncDAGModel(
        dag_id=name,
        owners="luke",
        schedule_interval=data["interval"],
        fileloc="",
        task_json_str=json.dumps(data["tasks"]),
        is_active=True,
        is_paused=False,
    )
    session.add(dag)
    session.commit()

    dag.refresh_dag_file()
    return jsonify({
        "code": 0,
        "redirect": "/dataxdagview/modify/%s" % name,
        "msg": "SUCCESS"
    })


@bp.route("/datax/api/dag/<dag_id>/export", methods=["GET"])
@provide_session
@csrf.exempt
def export_dag(dag_id, session=None):
    "导出dag"
    dag = session.query(SyncDAGModel).get(dag_id)
    if not dag:
        return jsonify({
            "code": -1,
            "msg": "不存在名为%s的dag" % dag_id
        })

    content = dag.dumps()

    fobj = NamedTemporaryFile(mode='w+b',suffix='json')
    fobj.write(content.encode("utf-8"))
    fobj.seek(0,0)
    return send_file(fobj,
                     attachment_filename='%s.json' % dag.dag_id,
                     as_attachment=True,)

@bp.route("/datax/api/dag/<dag_id>/trigger", methods=["POST"])
@provide_session
def trigger_dag(dag_id, session=None):
    dag = session.query(DagModel).filter(DagModel.dag_id == dag_id).first()
    if not dag:
        return jsonify({
            "code": -1,
            "msg": "不存在名为%s的dag" % dag_id
        })

    execution_date = timezone.utcnow()
    run_id = "manual__{0}".format(execution_date.isoformat())

    dr = DagRun.find(dag_id=dag_id, run_id=run_id)
    if dr:
        return jsonify({
            "code": -1,
            "msg": "This run_id {} already exists".format(run_id)
        })

    dr = DagRun.find(dag_id=dag_id, state="running")
    print("*********")
    print(dr)
    if len(dr) > 1:
        obj = dr[-1]
        return jsonify({
            "code": 0,
            "msg": "SUCCESS",
            "run_id": obj.run_id,
        })

    run_conf = {}
    dag.create_dagrun(
        run_id=run_id,
        execution_date=execution_date,
        state=State.RUNNING,
        conf=run_conf,
        external_trigger=True
    )

    return jsonify({
        "code": 0,
        "msg": "SUCCESS",
        "run_id": run_id,
    })


@bp.route("/datax/api/dag/<dag_id>/runinfo", methods=["GET"])
@provide_session
def get_dag_run_info(dag_id, session=None):
    """
    执行情况
    """
    latest_success = session.query(DagRun).filter(
        DagRun.dag_id == dag_id,
        DagRun._state == "success"
    ).order_by(DagRun.execution_date.desc()).first()
    return jsonify({
        "code": 0,
        "msg": "SUCCESS",
        "latest_success_start_date": latest_success.start_date.strftime("%Y-%m-%d %H:%M:%S"),
        "latest_success_execution_date": latest_success.execution_date.strftime("%Y-%m-%d %H:%M:%S"),
        "latest_sucdess_run_id": latest_success.run_id,
    })

bp.add_url_rule('/datax/api/syncdags', view_func=SyncDAGListView.as_view('syncdaglist'))
bp.add_url_rule('/datax/api/syncdag/<dag_id>', view_func=SyncDAGDetailView.as_view('syncdetaillist'))


class DataXDAGView(AppBuilderBaseView):

    @expose('/')
    @provide_session
    def list(self, session=None):
        currentPage = 1
        pageSize = 10
        allPages = 1
        qs = session.query(SyncDAGModel).all()
        dags = []
        for dag in qs:
            dags.append({
                "name": dag.dag_id,
                "interval": dag.schedule_interval,
                "state": dag.state,
            })
        return self.render_template("datax/list.html",
                                    dags=dags,
                                    pageSize=pageSize,
                                    allPages=allPages,
                                    currentPage=currentPage)

    @expose('/create')
    def dag_add_page(self):
        return self.render_template("datax/add_task.html",
                                    sync_types=SYNC_TYPES)

    @expose('/modify/<dag_id>')
    @provide_session
    def dag_modify_page(self, dag_id, session=None):
        dag = session.query(SyncDAGModel).get(dag_id)
        data = dag.to_json()

        return self.render_template("datax/modify_task.html",
                                    dag=data,
                                    sync_types=SYNC_TYPES)


datax_view = DataXDAGView()
appbuilder_views = {
    "name": "DAG列表",
    "category": "数据同步DAGs",
    "view": datax_view
}


class DataXPlugin(AirflowPlugin):
    name = "datax"
    operators = [RDMS2RDMSOperator]
    sensors = [PluginSensorOperator]
    hooks = [RDBMS2RDBMSFullHook, RDBMS2RDBMSAppendHook2]
    executors = [PluginExecutor]
    macros = [plugin_macro]
    # admin_views = [datax_view]
    flask_blueprints = [bp]
    # menu_links = [ml]
    appbuilder_views = [appbuilder_views]
    # appbuilder_menu_items = []
    # global_operator_extra_links = []

