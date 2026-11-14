from databricks.connect import DatabricksSession
import pyspark.sql.functions as F
import pytest

@pytest.fixture(scope="session")
def spark() -> DatabricksSession:
    return DatabricksSession.builder.getOrCreate()

def test_spark(spark: DatabricksSession):
    dados = [
        (1, 0.23, "Ideal", "E", "SI2", 61.5, 55.0, 326, 3.95, 3.98, 2.43),
        (2, 0.21, "Premium", "E", "SI1", 59.8, 61.0, 326, 3.89, 3.84, 2.31),
    ]
    colunas = [
        "_c0", "carat", "cut", "color", "clarity",
        "depth", "table", "price", "x", "y", "z",
    ]

    df = spark.createDataFrame(dados, colunas)
    data = df.filter(F.col("cut") == "Ideal")

    assert data.collect()[0][2] == "Ideal"