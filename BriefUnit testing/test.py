import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, to_date
from pyspark.sql.types import IntegerType, DoubleType

# Configuration PySpark pour pytest
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Créer une session Spark
spark_session = SparkSession.builder.master('local[1]').getOrCreate()

def test_transform_employee_data():
    # Données d'entrée
    input_matrix = [
        ('John', 'HR', 50000, '2015-01-01'),
        ('Jane', 'Finance', 60000, '2018-06-15'),
        ('Dave', 'IT', 55000, '2020-08-01'),
        ('Anna', 'Finance', 65000, '2019-11-23')
    ]
    
    # Données de sortie attendues
    expected_output_matrix = [
        ('John', 'HR', 50000, '2015-01-01', 2015, 55000.0),
        ('Jane', 'Finance', 60000, '2018-06-15', 2018, 66000.0),
        ('Dave', 'IT', 55000, '2020-08-01', 2020, 60500.0),
        ('Anna', 'Finance', 65000, '2019-11-23', 2019, 71500.0)
    ]

    # Colonnes d'entrée et de sortie attendues
    columns = ['Employee', 'Department', 'Salary', 'JoinDate']
    columns_expected_output = ['Employee', 'Department', 'Salary', 'JoinDate', 'JoinYear', 'SalaryIncrease']

    # Créer les DataFrames
    input_df = spark_session.createDataFrame(input_matrix, columns)
    expected_df = spark_session.createDataFrame(expected_output_matrix, columns_expected_output)

    # Transformation sur les colonnes
    df = input_df.withColumn('JoinYear', year(to_date(col('JoinDate'), 'yyyy-MM-dd'))) \
                 .withColumn('SalaryIncrease', col('Salary') * 1.10)

    # Sélectionner les colonnes dans le même ordre pour la comparaison
    output_df = df.select(*columns_expected_output)

    # Assurez-vous que les colonnes ont les bons types de données pour la comparaison
    output_df = output_df.withColumn("JoinYear", col("JoinYear").cast(IntegerType())) \
                         .withColumn("SalaryIncrease", col("SalaryIncrease").cast(IntegerType()))
    expected_df = expected_df.withColumn("JoinYear", col("JoinYear").cast(IntegerType())) \
                             .withColumn("SalaryIncrease", col("SalaryIncrease").cast(IntegerType()))

    output_df.show()
    expected_df.show()
    # Trouver les lignes qui sont dans expected_df mais pas dans output_df
    diff_df = expected_df.subtract(output_df)
    diff_count = diff_df.count()
    
    # Imprimer les différences pour le diagnostic
    if diff_count > 0:
        print("Differences in DataFrames:")
        diff_df.show(truncate=False)

    # Validation avec subtract pour comparer les DataFrames
    assert diff_count == 0, "Les DataFrames ne correspondent pas"

