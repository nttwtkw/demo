task1= BashOperator(
        task_id='Test_kubernetes_executor',
        bash_command='echo Kubernetes',
        queue = 'kubernetes'
    )
task2 = BashOperator(
        task_id='Test_Celery_Executor',
        bash_command='echo Celery',
    )
