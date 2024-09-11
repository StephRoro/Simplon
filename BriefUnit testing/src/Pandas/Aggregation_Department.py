from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, sum

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
    sum('visit_count').alias('total_visits'),
    avg('age').alias('avg_age'),
    max('age').alias('max_age')
)

#agg_df = df.groupBy('department').agg(
#    sum('visit_count'),
#    avg('age'),
#    max('age')
#        ).withColumnRenamed('sum(visit_count)', 'total_visits'
#                            ).withColumnRenamed('avg(age)', 'avg_age'
#                                                ).withColumnRenamed('max(age)', 'max_age')

agg_df.show()