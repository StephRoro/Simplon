from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("EmployeeData").getOrCreate()

# Données
data = [(1, 34, 'Cardiology'), 
        (2, 70, 'Neurology'), 
        (3, 50, 'Orthopedics'), 
        (4, 20, 'Cardiology'), 
        (5, 15, 'Neurology')]
columns = ['patient_id', 'age', 'department']

df = spark.createDataFrame(data, columns)

# Ajout d'une colonne conditionnelle (catégorie d'âge)
df = df.withColumn('age_category', 
                  F. when(df['age'] > 60, 'senior')
                   .when(df['age'] > 18, 'adult')
                   .otherwise('minor'))

df.show()