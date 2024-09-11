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
            ('Cardiology', 1000), 
            ('Neurology', 2000), 
            ('Orthopedics', 1500), 
            ('Cardiology', 1200), 
            ('Neurology', 1800)
            ]
    
    # Données de sortie attendues
    expected_output_matrix = [
                    ('Cardiology', 2200),
                     ('Neurology', 3800),
                     ('Orthopedics', 1500)
                     ]

    # Colonnes d'entrée et de sortie attendues
    columns =  ['department', 'cost']
    columns_expected_output =  ['department', 'total_cost']

    # Créer les DataFrames
    input_df = spark_session.createDataFrame(input_matrix, columns)
    expected_df = spark_session.createDataFrame(expected_output_matrix, columns_expected_output)

    # Transformation sur les colonnes
    df = input_df.groupBy('department').agg({'cost': 'sum'}).withColumnRenamed('sum(cost)', 'total_cost')

    # Sélectionner les colonnes dans le même ordre pour la comparaison
    output_df = df.select(*columns_expected_output)

    output_df.show()
    expected_df.show()

    # Validation avec subtract pour comparer les DataFrames
    assert expected_df.subtract(output_df).count() == 0