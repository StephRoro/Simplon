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
    input_matrix = [('John Doe', 'Diabetes'),
        ('Jane Smith', 'Heart Disease'),
        ('Alice Brown', 'Hypertension')]

    # Transformation attendue
    expected_output_matrix = [('John Doe', 'diabetes', 'John Doe - diabetes'),
                     ('Jane Smith', 'heart disease', 'Jane Smith - heart disease'),
                     ('Alice Brown', 'hypertension', 'Alice Brown - hypertension')]
    
   
    columns = ['patient_name', 'diagnosis']
    columns_expected_output = ['patient_name', 'diagnosis_lower', 'full_info']
    
    # Créer les DataFrames
    input_df = spark_session.createDataFrame(input_matrix, columns)
    expected_df = spark_session.createDataFrame(expected_output_matrix, columns_expected_output)
    
    # Transformation sur les colonnes
    df = input_df.withColumn('diagnosis_lower', F.lower(F.col('diagnosis'))
                  ).withColumn('full_info', F.concat_ws(' - ', F.col('patient_name'), F.col('diagnosis_lower')))

    # Sélectionner les colonnes dans le même ordre pour la comparaison
    output_df = df.select(*columns_expected_output)

    output_df.show()
    expected_df.show()

    # Validation avec subtract pour comparer les DataFrames
    assert expected_df.subtract(output_df).count() == 0