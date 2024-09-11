from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("EmployeeData").getOrCreate()

# Données
data = [('John Doe', 'Diabetes'),
        ('Jane Smith', 'Heart Disease'),
        ('Alice Brown', 'Hypertension')]
columns = ['patient_name', 'diagnosis']

df = spark.createDataFrame(data, columns)

# Transformation en PySpark
df = df.withColumn('diagnosis_lower', F.lower(F.col('diagnosis')))
df = df.withColumn('full_info', F.concat_ws(' - ', F.col('patient_name'), F.col('diagnosis_lower')))

df.show()