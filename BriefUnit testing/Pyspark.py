from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, to_date

# Créer une session Spark
spark = SparkSession.builder.appName("EmployeeData").getOrCreate()

# Créer un DataFrame
data = [
    ('John', 'HR', 50000, '2015-01-01'),
    ('Jane', 'Finance', 60000, '2018-06-15'),
    ('Dave', 'IT', 55000, '2020-08-01'),
    ('Anna', 'Finance', 65000, '2019-11-23')
]

columns = ['Employee', 'Department', 'Salary', 'JoinDate']
df = spark.createDataFrame(data, columns)

# Ajouter une colonne pour l'année d'adhésion
df = df.withColumn('JoinYear', year(to_date(col('JoinDate'))))

# Calculer le salaire après augmentation de 10%
df = df.withColumn('SalaryIncrease', col('Salary') * 1.10)

# Grouper par département et obtenir le salaire moyen
grouped = df.groupBy('Department').agg({'SalaryIncrease': 'avg'})

# Filtrer les employés qui ont rejoint avant 2018
filtered_df = df.filter(col('JoinYear') < 2018)

filtered_df.show()