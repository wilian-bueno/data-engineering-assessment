"""
DAG: assessment_pipeline
────────────────────────
Single pipeline covering all stages of the data engineering assessment.

Notes:
  · Each ingestion task is a ShortCircuit — skips load when file is unchanged.
  · Transform tasks use NONE_FAILED trigger rule — run even when ingest was skipped.
  · publish_orders waits for BOTH sales order transforms before running.
"""

import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

sys.path.insert(0, "/opt/airflow")

from include.tasks.ingestion_tasks import (
    init_metadata_table,
    file_needs_load,
    load_raw_file,
)
from include.tasks.product_tasks import (
    create_store_product_schema,
    load_store_product_master,
    create_publish_product_schema,
    load_publish_product,
)
from include.tasks.validation_tasks import run_full_pipeline_validation
from include.tasks.analysis_tasks import run_revenue_by_color_analysis, run_avg_lead_time_analysis
from include.utils.quality_utils import (
    dq_check_store_product_master,
    dq_check_store_sales_order_header,
    dq_check_store_sales_order_detail,
)
from include.tasks.sales_order_tasks import (
    create_store_sales_order_header_schema,
    load_store_sales_order_header,
    create_store_sales_order_detail_schema,
    load_store_sales_order_detail,
    create_publish_orders_schema,
    load_publish_orders,
)

# ─────────────────────────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────────────────────────

default_args = {
    "owner": "Wilian Bueno",
    "retries": 1,
    "email_on_failure": False,
}

with DAG(
    dag_id="assessment_pipeline",
    description="Full assessment pipeline: ingest → store → DQ → publish for all tables.",
    default_args=default_args,
    start_date=datetime(2026, 6, 5),
    schedule_interval=None,
    catchup=False, # it will not catch up on missed runs
    tags=["assessment", "ingestion", "transform", "publish"],
) as dag:

    start = EmptyOperator(task_id="start")

    init_metadata = PythonOperator(
        task_id="init_metadata_table",
        python_callable=init_metadata_table,
    )

    # ── Analysis: Q1 and Q2 — run after both publish tables exist ────────────
    with TaskGroup("run_analysis") as run_analysis:

        q1 = PythonOperator(
            task_id="revenue_by_color_year",
            python_callable=run_revenue_by_color_analysis,
            trigger_rule=TriggerRule.NONE_FAILED,
        )

        q2 = PythonOperator(
            task_id="avg_lead_time_by_category",
            python_callable=run_avg_lead_time_analysis,
        )

        # Q1 and Q2 are independent — run in parallel
        [q1, q2]

    # ── Final validation ─────────────
    with TaskGroup("final_validation") as final_validation:
        validate = PythonOperator(
            task_id="run_full_pipeline_validation",
            python_callable=run_full_pipeline_validation,
            trigger_rule=TriggerRule.ALL_DONE,  # always runs, even if some tasks were skipped
        )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ─────────────────────────────────────────────────────────────────────────
    # PRODUCTS chain
    # ─────────────────────────────────────────────────────────────────────────

    with TaskGroup("ingest_products") as ingest_products:
        check_p = ShortCircuitOperator(
            task_id="check_products_file",
            python_callable=file_needs_load,
            op_kwargs={"file_key": "products"},
        )
        load_p = PythonOperator(
            task_id="load_raw_products",
            python_callable=load_raw_file,
            op_kwargs={"file_key": "products"},
        )
        check_p >> load_p

    with TaskGroup("transform_products") as transform_products:

        with TaskGroup("store_products") as store_products:
            create_store_p = PythonOperator(
                task_id="create_store_schema",
                python_callable=create_store_product_schema,
                trigger_rule=TriggerRule.NONE_FAILED,
            )
            load_store_p = PythonOperator(
                task_id="load_store_product_master",
                python_callable=load_store_product_master,
            )
            create_store_p >> load_store_p

        with TaskGroup("quality_check_products") as quality_p:
            dq_p = PythonOperator(
                task_id="dq_check_store_product_master",
                python_callable=dq_check_store_product_master,
            )

        with TaskGroup("publish_products") as publish_p:
            create_pub_p = PythonOperator(
                task_id="create_publish_schema",
                python_callable=create_publish_product_schema,
            )
            load_pub_p = PythonOperator(
                task_id="load_publish_product",
                python_callable=load_publish_product,
            )
            create_pub_p >> load_pub_p

        store_products >> quality_p >> publish_p

    # ─────────────────────────────────────────────────────────────────────────
    # SALES ORDER HEADER 
    # ─────────────────────────────────────────────────────────────────────────

    with TaskGroup("ingest_sales_order_header") as ingest_header:
        check_h = ShortCircuitOperator(
            task_id="check_sales_order_header_file",
            python_callable=file_needs_load,
            op_kwargs={"file_key": "sales_order_header"},
        )
        load_h = PythonOperator(
            task_id="load_raw_sales_order_header",
            python_callable=load_raw_file,
            op_kwargs={"file_key": "sales_order_header"},
        )
        check_h >> load_h

    with TaskGroup("transform_sales_order_header") as transform_header:

        with TaskGroup("store_sales_order_header") as stage_h:
            create_stage_h = PythonOperator(
                task_id="create_store_schema",
                python_callable=create_store_sales_order_header_schema,
                trigger_rule=TriggerRule.NONE_FAILED,
            )
            load_stage_h = PythonOperator(
                task_id="load_store_sales_order_header",
                python_callable=load_store_sales_order_header,
            )
            create_stage_h >> load_stage_h

        with TaskGroup("quality_check_sales_order_header") as quality_h:
            dq_h = PythonOperator(
                task_id="dq_check_store_sales_order_header",
                python_callable=dq_check_store_sales_order_header,
            )

        stage_h >> quality_h

    # ─────────────────────────────────────────────────────────────────────────
    # SALES ORDER DETAIL
    # ─────────────────────────────────────────────────────────────────────────

    with TaskGroup("ingest_sales_order_detail") as ingest_detail:
        check_d = ShortCircuitOperator(
            task_id="check_sales_order_detail_file",
            python_callable=file_needs_load,
            op_kwargs={"file_key": "sales_order_detail"},
        )
        load_d = PythonOperator(
            task_id="load_raw_sales_order_detail",
            python_callable=load_raw_file,
            op_kwargs={"file_key": "sales_order_detail"},
        )
        check_d >> load_d

    with TaskGroup("transform_sales_order_detail") as transform_detail:

        with TaskGroup("store_sales_order_detail") as stage_d:
            create_stage_d = PythonOperator(
                task_id="create_store_schema",
                python_callable=create_store_sales_order_detail_schema,
                trigger_rule=TriggerRule.NONE_FAILED,
            )
            load_stage_d = PythonOperator(
                task_id="load_store_sales_order_detail",
                python_callable=load_store_sales_order_detail,
            )
            create_stage_d >> load_stage_d

        with TaskGroup("quality_check_sales_order_detail") as quality_d:
            dq_d = PythonOperator(
                task_id="dq_check_store_sales_order_detail",
                python_callable=dq_check_store_sales_order_detail,
            )

        stage_d >> quality_d

    # ─────────────────────────────────────────────────────────────────────────
    # PUBLISH ORDERS
    # ─────────────────────────────────────────────────────────────────────────

    with TaskGroup("publish_orders") as publish_orders:
        create_pub_o = PythonOperator(
            task_id="create_publish_orders_schema",
            python_callable=create_publish_orders_schema,
            trigger_rule=TriggerRule.NONE_FAILED,
        )
        load_pub_o = PythonOperator(
            task_id="load_publish_orders",
            python_callable=load_publish_orders,
        )
        create_pub_o >> load_pub_o


    start >> init_metadata

    # Products: fully independent chain
    init_metadata >> ingest_products >> transform_products

    # Orders: header and detail run in parallel, converge at publish_orders
    init_metadata >> ingest_header >> transform_header
    init_metadata >> ingest_detail >> transform_detail
    [transform_header, transform_detail] >> publish_orders

    # Analysis runs after both publish tables are ready (parallel Q1 + Q2)
    [transform_products, publish_orders] >> run_analysis

    # Final validation runs after analysis
    run_analysis >> final_validation >> end
