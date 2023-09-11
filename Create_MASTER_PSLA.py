import glob
import pandas as pd

path = 'C:/Users/RTLSAdmin/Documents/GitHub/AccessLogConversion/DailyLogs/ModifiedPSLA7*.csv'
csv_files = glob.glob(path)
print(csv_files)

df = pd.DataFrame()

for file in csv_files:
    data = pd.read_csv(file)
    df = pd.concat([df, data], axis = 0)

df.to_csv('PSLA_Master.csv', index = False)