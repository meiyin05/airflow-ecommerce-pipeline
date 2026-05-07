from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time

# 1. 定义默认参数
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 2. 定义具体的电商数据处理逻辑
def extract_orders():
    print("正在连接电商生产数据库 (MySQL)...")
    time.sleep(2)  # 模拟网络延迟
    print("成功抓取昨日新增订单: 1,240 条。")

def transform_data():
    print("正在进行数据清洗...")
    print("换算货币单位：USD -> CNY...")
    print("剔除已退款订单...")

def load_to_warehouse():
    print("正在将清洗后的数据写入数据仓库 (Postgres)...")
    print("数据加载完成，准备生成今日销售报表。")

# 3. 实例化 DAG
with DAG(
    'ecommerce_data_pipeline_v1',
    default_args=default_args,
    description='电商订单数据抽取-清洗-加载全流程',
    schedule_interval=timedelta(days=1), # 每天跑一次
    catchup=False,
    tags=['ecommerce', 'sales'],
) as dag:

    # 4. 定义任务节点
    t1 = PythonOperator(task_id='extract_from_source', python_callable=extract_orders)
    t2 = PythonOperator(task_id='transform_sales_data', python_callable=transform_data)
    t3 = PythonOperator(task_id='load_to_dw', python_callable=load_to_warehouse)

    # 5. 设置任务之间的依赖关系（顺序）
    t1 >> t2 >> t3