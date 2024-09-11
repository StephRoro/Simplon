import pandas as pd
import numpy as np

# Créer un DataFrame
data = {
    'Employee': ['John', 'Jane', 'Dave', 'Anna'],
    'Department': ['HR', 'Finance', 'IT', 'Finance'],
    'Salary': [50000, 60000, 55000, 65000],
    'JoinDate': ['2015-01-01', '2018-06-15', '2020-08-01', '2019-11-23']
}

df = pd.DataFrame(data)

# Ajouter une colonne pour l'année d'adhésion
df['JoinYear'] = pd.to_datetime(df['JoinDate']).dt.year

# Calculer le salaire après augmentation de 10%
df['SalaryIncrease'] = df['Salary'] * 1.10

# Grouper par département et obtenir le salaire moyen
grouped = df.groupby('Department')['SalaryIncrease'].mean().reset_index()

# Filtrer les employés qui ont rejoint avant 2018
filtered_df = df[df['JoinYear'] < 2018]

filtered_df