import glob
import pandas as pd

path = 'C:/Users/RTLSAdmin/Downloads/PSLA7*.csv'
csv_files = glob.glob(path)
print(csv_files)

df = pd.DataFrame()

for file in csv_files:
    data = pd.read_csv(file)
    df = pd.concat([df, data], axis = 0)

df.to_csv("All_PSLA_Logs", index = False)