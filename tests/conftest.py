import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    s = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    s.sparkContext.setLogLevel("WARN")
    yield s
    s.stop()
