# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from datetime import datetime

from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.dbt.cloud.operators.dbt import (
    DbtCloudRunJobOperator,
)

with DAG(
    dag_id="dbt_cloud_provider_fct_orders",
    default_args={"dbt_cloud_conn_id": "dbt_cloud", "account_id": 28885},
    start_date=datetime.today(),
    schedule_interval=None,
    catchup=False,
) as dag:
    extract = DummyOperator(task_id="extract")
    load = DummyOperator(task_id="load")
    build_fct_orders = DummyOperator(task_id="build_fct_orders")

    trigger_dbt_cloud_job_run = DbtCloudRunJobOperator(
        task_id="trigger_dbt_cloud_job_run",
        trigger_reason="Triggered from Airflow",
        job_id=197202,
        check_interval=10,
        timeout=300,
    )

    extract >> load >> trigger_dbt_cloud_job_run >> build_fct_orders
