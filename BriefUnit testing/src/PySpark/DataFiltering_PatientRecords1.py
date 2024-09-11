from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("EmployeeData").getOrCreate()

data = [(1, 34, 'Cardiology'), 
        (2, 45, 'Neurology'), 
        (3, 50, 'Orthopedics'), 
        (4, 20, 'Cardiology'), 
        (5, 15, 'Neurology')]
columns = ['patient_id', 'age', 'department']

df = spark.createDataFrame(data, columns)

# Filtrer les patients âgés de plus de 30 ans
filtered_df = df.filter(F.col('age') > 30)

filtered_df.show()