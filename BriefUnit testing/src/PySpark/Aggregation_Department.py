from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('Healthcare').getOrCreate()

# Données
data = [(1, 34, 'Cardiology', 10), 
        (2, 45, 'Neurology', 12), 
        (3, 23, 'Cardiology', 5), 
        (4, 64, 'Orthopedics', 8), 
        (5, 52, 'Cardiology', 9)]
columns = ['patient_id', 'age', 'department', 'visit_count']

df = spark.createDataFrame(data, columns)

# GroupBy et calculs statistiques
agg_df = df.groupBy('department').agg(
    F.sum('visit_count').alias('total_visits'),
    F.avg('age').alias('avg_age'),
    F.max('age').alias('max_age')
)

agg_df.show()