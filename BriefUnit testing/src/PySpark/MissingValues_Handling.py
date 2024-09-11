from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("EmployeeData").getOrCreate()

# Données avec des valeurs manquantes
data = [(1, 34, 'Cardiology'), 
        (2, None, 'Neurology'), 
        (3, 50, 'Orthopedics'), 
        (4, None, None), 
        (5, 15, 'Neurology')]
columns = ['patient_id', 'age', 'department']

df = spark.createDataFrame(data, columns)

# Remplacement des valeurs manquantes
age_mean = df.select(F.avg(F.col('age'))).first()[0]

df = df.fillna({'age': age_mean, 'department': 'Unknown'})

df.show()