import pytest
from airflow.models import DagBag
import os
os.environ['AIRFLOW__CORE__LOAD_EXAMPLES'] = 'False'

@pytest.fixture
def dagbag():
    return DagBag(dag_folder="./dags", include_examples=False)

def test_dag_integrity(dagbag):
    """Проверяем, что DAG загружается без ошибок"""
    assert len(dagbag.import_errors) == 0, f"Ошибки импорта: {dagbag.import_errors}"
    assert 'superstore_etl' in dagbag.dags

def test_dag_structure(dagbag):
    """Проверяем структуру DAG"""
    dag = dagbag.dags.get('superstore_etl')
    assert dag is not None
    
    task_ids = [task.task_id for task in dag.tasks]
    expected_tasks = ['load_to_bronze', 'run_dbt_transformations', 'test_data_quality']
    
    for task_id in expected_tasks:
        assert task_id in task_ids