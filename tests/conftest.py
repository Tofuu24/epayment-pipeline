"""Shared fixtures. Tests run Spark locally (pip install -r requirements.txt); no Docker,
Kafka or Cassandra needed. Run from the project folder:  python -m pytest tests -q"""
import os
import sys
import time

import pytest

SPARK_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "spark")
sys.path.insert(0, SPARK_DIR)
# Python workers are separate processes; they need the job modules on their path too.
os.environ["PYTHONPATH"] = SPARK_DIR + os.pathsep + os.environ.get("PYTHONPATH", "")
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
# collect() converts timestamps to the local time zone; make that Manila so test
# expectations read like the business rules (10:00, 13:00, 16:00).
os.environ["TZ"] = "Asia/Manila"
time.tzset()


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master("local[2]").appName("tests")
         .config("spark.sql.session.timeZone", "Asia/Manila")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture(scope="session")
def registry(spark):
    return spark.createDataFrame([
        ("BANK_UNIONBANK", True, ["INSTAPAY", "PESONET"]),
        ("BANK_BPI", True, ["INSTAPAY", "PESONET"]),
        ("BANK_LANDBANK", True, ["PESONET"]),
        ("EMI_GCASH", True, ["INSTAPAY"]),
        ("EMI_MAYA", True, ["INSTAPAY"]),
        ("BANK_CLOSED", False, ["INSTAPAY", "PESONET"]),
    ], "institution_code string, active boolean, rail_eligibility array<string>")
