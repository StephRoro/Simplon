import os
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, DoubleType

# Configuration PySpark pour pytest
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Créer une session Spark
spark_session = SparkSession.builder.master('local[1]').getOrCreate()

def test_agg_df():
    # Données d'entrée
    input_matrix = [
            (1, 34, 'Cardiology'), 
            (2, 70, 'Neurology'), 
            (3, 50, 'Orthopedics'), 
            (4, 20, 'Cardiology'), 
            (5, 15, 'Neurology')
            ]

    # Transformation attendue
    expected_output_matrix =  [
            (1, 34, 'Cardiology', 'adult'),
            (2, 70, 'Neurology', 'senior'),
            (3, 50, 'Orthopedics', 'adult'),
            (4, 20, 'Cardiology', 'adult'),
            (5, 15, 'Neurology', 'minor')
            ]
    
   
    columns = ['patient_id', 'age', 'department']
    columns_expected_output =  ['patient_id', 'age', 'department', 'age_category']
    
    # Créer les DataFrames
    input_df = spark_session.createDataFrame(input_matrix, columns)
    expected_df = spark_session.createDataFrame(expected_output_matrix, columns_expected_output)
    
    # Transformation sur les colonnes
    df = input_df.withColumn('age_category', 
                  F. when(F.col('age') > 60, 'senior')
                   .when(F.col('age') > 18, 'adult')
                   .otherwise('minor'))
    
    # Sélectionner les colonnes dans le même ordre pour la comparaison
    output_df = df.select(*columns_expected_output)

    output_df.show()
    expected_df.show()

    # Validation avec subtract pour comparer les DataFrames
    assert expected_df.subtract(output_df).count() == 0