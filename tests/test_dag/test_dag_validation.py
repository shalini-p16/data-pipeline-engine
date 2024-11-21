from airflow.models import DagBag

def test_dag_loaded():
    dagbag = DagBag()
    dag = dagbag.get_dag('kafka_spark_pipeline')
    assert dag is not None
    assert len(dag.tasks) > 0
