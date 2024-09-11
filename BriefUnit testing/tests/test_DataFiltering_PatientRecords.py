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
    input_matrix = [(1, 34, 'Cardiology'), 
        (2, 45, 'Neurology'), 
        (3, 50, 'Orthopedics'), 
        (4, 20, 'Cardiology'), 
        (5, 15, 'Neurology')]

    # Transformation attendue
    expected_output_matrix =  [(1, 34, 'Cardiology'),
                     (2, 45, 'Neurology'),
                     (3, 50, 'Orthopedics')]
    
   
    columns = ['patient_id', 'age', 'department']
    columns_expected_output =  ['patient_id', 'age', 'department']
    
    # Créer les DataFrames
    input_df = spark_session.createDataFrame(input_matrix, columns)
    expected_df = spark_session.createDataFrame(expected_output_matrix, columns_expected_output)
    
    # Transformation sur les colonnes
    df = input_df.filter(F.col('age') > 30)
    
    # Sélectionner les colonnes dans le même ordre pour la comparaison
    output_df = df.select(*columns_expected_output)

    output_df.show()
    expected_df.show()

    # Validation avec subtract pour comparer les DataFrames
    assert expected_df.subtract(output_df).count() == 0