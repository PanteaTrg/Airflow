from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from utils.teams_webhook import teams_on_failure_callback


SCRIPT_DIR = ""
PYTHON = ""
default_args = {"on_failure_callback": teams_on_failure_callback,}

with DAG(
        dag_id="LPN_KYC",
        start_date=datetime(2026, 11, 2),
        schedule="@daily",
        catchup=False,
        default_args = default_args,
        tags=["migration", "mongodb", "oracle", "bi", ""],
        doc_md="MongoDB43 to Oracle; full reload; first Load Plan step only.",

) as dag:
    task_1 = BashOperator(
        task_id="DEMAND_JOURNALS_PY",
        bash_command=f'"{PYTHON}" "{SCRIPT_DIR}/SMC_CREDIT_DEMAND_JOURNALS.py"',
    )

    task_2 = BashOperator(
        task_id="CHEQUES_INQUIRY_JOURNALS_NEW",
        bash_command=f'"{PYTHON}" "{SCRIPT_DIR}/sayadi_cheques_inquiry_journals.py"',
    )

    task_3 = BashOperator(
        task_id="INQ_JOURNALS_PY",
        bash_command=f'"{PYTHON}" "{SCRIPT_DIR}/CHEQUE_INQUIRY_JOURNALS.py"',
    )

    task_4 = BashOperator(
        task_id="NOTES_PY",
        bash_command=f'"{PYTHON}" "{SCRIPT_DIR}/PROMISSORY_NOTES.py"',
    )

    task_5 = BashOperator(
        task_id="SIGNATURE_PY",
        bash_command=f'"{PYTHON}" "{SCRIPT_DIR}/DIGITAL_SIGNATURES.py"',
    )

    task_6 = BashOperator(
        task_id="PAYMENTS_PY",
        bash_command=f'"{PYTHON}" "{SCRIPT_DIR}/SIGNATURE_PAYMENTS.py"',
    )

    task_7 = BashOperator(
        task_id="DETAIL_JOURNALS_PY",
        bash_command=f'"{PYTHON}" "{SCRIPT_DIR}/KYC_ADDRESS_DETAIL_JOURNALS.py"',
    )

    task_8 = BashOperator(
        task_id="INQUERY_JOURNAL_PY",
        bash_command=f'"{PYTHON}" "{SCRIPT_DIR}/SANACODE_INQUERY_JOURNALS.py"',
    )

    task_9 = BashOperator(
        task_id="INQUIRY_JOURNALS_PY",
        bash_command=f'"{PYTHON}" "{SCRIPT_DIR}/BOUNCED_CHQ_INQUIRY_JOURNAL.py"',
    )

    task_10 = BashOperator(
        task_id="INQUIRY_JOURNALS_PY",
        bash_command=f'"{PYTHON}" "{SCRIPT_DIR}/SALARY_RANK_INQUIRY_JOURNALS.py"',
    )

    task_11 = BashOperator(
        task_id="INQUIRY_JOURNALS_PY",
        bash_command=f'"{PYTHON}" "{SCRIPT_DIR}/POSTPON_LOAN_INQ_JOURNAL.py"',
    )

    task_12 = BashOperator(
        task_id="INQUIRY_JOURNALS_PY",
        bash_command=f'"{PYTHON}" "{SCRIPT_DIR}/KYC_REGISTRATION_INQUIRY.py"',
    )

    task_13 = BashOperator(
        task_id="REQUEST_JOURNALS_PY",
        bash_command=f'"{PYTHON}" "{SCRIPT_DIR}/KYC_SHAHKAR_REQUEST_JOURNALS.py"',
    )

    task_14 = BashOperator(
        task_id="IBAN_INFO_JOURNALA",
        bash_command=f'"{PYTHON}" "{SCRIPT_DIR}/iban_inquiry_journals.py"',
    )

    task_15 = BashOperator(
        task_id="PROFILE_JOURNAL_PY",
        bash_command=f'"{PYTHON}" "{SCRIPT_DIR}/SW_CARD_PROFILE_JOURNAL.py"',
    )

    task_16 = BashOperator(
        task_id="INFO_JOURNAL_PY",
        bash_command=f'"{PYTHON}" "{SCRIPT_DIR}/SW_CARD_IBAN_INFO_JOURNAL.py"',
    )

    task_17 = BashOperator(
        task_id="PKG_SW_IBAN_INFO_JOURNAL_PY",
        bash_command=f'"{PYTHON}" "{SCRIPT_DIR}/SW_IBAN_INFO_JOURNAL.py"',
    )

    (  task_1
        >> task_2
        >> task_3
        >> task_4
        >> task_5
        >> task_6
        >> task_7
        >> task_8
        >> task_9
        >> task_10
        >> task_11
        >> task_12
        >> task_13
        >> task_14
        >> task_15
        >> task_16
        >> task_17
    )