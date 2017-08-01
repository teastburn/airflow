# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import logging
from datetime import timedelta

from airflow.models import TaskInstance

from airflow import settings
from airflow.utils.state import State

from airflow.utils.decorators import apply_defaults

from airflow.operators.sensors import BaseSensorOperator


class ExternalTaskConsecutiveRunSensor(BaseSensorOperator):
    """
    Waits for a task to complete in a different DAG

    :param external_dag_id: The dag_id that contains the task you want to
        wait for
    :type external_dag_id: string
    :param external_task_id: The task_id that contains the task you want to
        wait for
    :type external_task_id: string
    :param allowed_states: list of allowed states, default is ``['success']``
    :type allowed_states: list
    :param lookback_delta: how far to look back. e.g., to check
        for a day's worth of runs, use timedelta(days=1)
    :type lookback_delta: datetime.timedelta
    :param expected runs: how many runs we expected to see within the
        lookback window
    :type expected_runs: int
    """

    @apply_defaults
    def __init__(
            self,
            external_dag_id,
            external_task_id,
            allowed_states=None,
            lookback_delta=None,
            expected_runs=1,
            *args, **kwargs):
        super(ExternalTaskConsecutiveRunSensor, self).__init__(*args, **kwargs)
        self.allowed_states = allowed_states or [State.SUCCESS]
        self.lookback_delta = lookback_delta or timedelta()
        self.expected_runs = expected_runs
        self.external_dag_id = external_dag_id
        self.external_task_id = external_task_id
        self.kwargs = kwargs

    def poke(self, context):
        schedule_interval = self.kwargs['dag'].schedule_interval
        start_dttm = context['execution_date'] - self.lookback_delta
        end_dttm = context['execution_date'] + schedule_interval

        logging.info(
            'Poking for '
            '{self.external_dag_id}.'
            '{self.external_task_id} in range '
            '{start_dttm} to {end_dttm} -- '
            'Expecting {self.expected_runs} runs ...'.format(**locals()))
        ti = TaskInstance

        session = settings.Session()
        count = session.query(ti).filter(
            ti.dag_id == self.external_dag_id,
            ti.task_id == self.external_task_id,
            ti.state.in_(self.allowed_states),
            ti.execution_date >= start_dttm,
            ti.execution_date < end_dttm
        ).count()
        logging.info('Found {count} runs'.format(**locals()))
        session.commit()
        session.close()
        return count == self.expected_runs
