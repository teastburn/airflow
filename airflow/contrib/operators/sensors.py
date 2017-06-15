from __future__ import print_function

from future import standard_library

standard_library.install_aliases()
import logging

from airflow import settings
from airflow.models import TaskInstance
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
            expected_runs=None,
            *args, **kwargs):
        super(ExternalTaskConsecutiveRunSensor, self).__init__(*args, **kwargs)
        self.allowed_states = allowed_states or [State.SUCCESS]
        self.lookback_delta = lookback_delta
        self.expected_runs = expected_runs
        self.external_dag_id = external_dag_id
        self.external_task_id = external_task_id

    def poke(self, context):
        start_dttm = context['execution_date'] - self.lookback_delta
        end_dttm = context['execution_date']

        logging.info(
            'Poking for '
            '{self.external_dag_id}.'
            '{self.external_task_id} in range '
            '{start_dttm} to {end_dttm} -- '
            'Expecting {self.expected_runs} runs ...'.format(**locals()))
        TI = TaskInstance

        session = settings.Session()
        count = session.query(TI).filter(
            TI.dag_id == self.external_dag_id,
            TI.task_id == self.external_task_id,
            TI.state.in_(self.allowed_states),
            TI.execution_date >= start_dttm,
            TI.execution_date < end_dttm
        ).count()
        logging.info('Found {count} runs'.format(**locals()))
        session.commit()
        session.close()
        return count == self.expected_runs