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
    input_matrix = [(1, 34, 'Cardiology', 10), 
            (2, 45, 'Neurology', 12), 
            (3, 23, 'Cardiology', 5), 
            (4, 64, 'Orthopedics', 8), 
            (5, 52, 'Cardiology', 9)]

    # Transformation attendue
    expected_output_matrix = [('Cardiology', 24, 36.33, 52), 
                     ('Neurology', 12, 45.0, 45), 
                     ('Orthopedics', 8, 64.0, 64)]
    
   
    columns = ['patient_id', 'age', 'department', 'visit_count']
    columns_expected_output = ['department', 'total_visits', 'avg_age', 'max_age']
    
    # Créer les DataFrames
    input_df = spark_session.createDataFrame(input_matrix, columns)
    expected_df = spark_session.createDataFrame(expected_output_matrix, columns_expected_output)
    
    # Transformation sur les colonnes
    df = input_df.groupBy('department').agg(
        F.sum('visit_count').alias('total_visits'),
        F.round(F.avg('age'),2).alias('avg_age'),
        F.max('age').alias('max_age')
    )

    # Sélectionner les colonnes dans le même ordre pour la comparaison
    output_df = df.select(*columns_expected_output)

    output_df.show()
    expected_df.show()

    # Validation avec subtract pour comparer les DataFrames
    assert expected_df.subtract(output_df).count() == 0