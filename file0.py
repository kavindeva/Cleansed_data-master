import pandas as pd
import os
import numpy as np

# import shutil

directory = "C:/Users/kavin/Documents/LANInnovations/Testing 1"
os.chdir("C:/Users/kavin/Documents/LANInnovations/Testing 1")
myFile = "94-smp-server_2020-05-27-0.log"
print(os.path.join(directory, myFile))
base = os.path.splitext(myFile)[0]
os.rename(myFile, base + ".txt")
txtFileName = base + ".txt"
txtFileTrimmed = base + "_LineTrimmed" + ".txt"
colNames = base + "_Columns.txt"
splitColNames = base + "_column1.txt"
filteredDataCSV = base + ".csv"
errorCSV = base + "_Error.csv"
with open(txtFileName) as old, open(txtFileTrimmed, 'w') as new, open(colNames, 'w') as Col:
    lines = old.readlines()
    new.writelines(lines[11:])
    Col.writelines(lines[6])

# Converting the Column text file into data table
with open(colNames) as old, open(splitColNames, 'w') as col1:
    df = pd.read_csv(colNames, header=None, sep=':')
    lines1 = df[1]
    df.head(5)
    col1.writelines(lines1)

# Separating the column into data table
df1 = pd.read_csv(splitColNames, header=None, sep='|')
columnList = df1.values.tolist()[0]

# Converting the text file into data table
df2 = pd.read_csv(txtFileTrimmed, header=None, sep='\n')
df2 = df2[0].str.split('#', expand=True)

if len(df2.columns) == len(columnList):
    # Add Column Name to a dataframe
    df2.columns = columnList
    # Dropping last column
    df2.drop(df2.columns[15], axis=1, inplace=True)
elif len(df2.columns) == (len(columnList) - 1):
    # Add Column Name to a dataframe
    df2.columns = columnList[:-1]
    # Dropping last column
    df2.drop(df2.columns[14], axis=1, inplace=True)
elif len(df2.columns) == (len(columnList) + 1):
    columnList.append('nan')
    # Add Column Name to a dataframe
    df2.columns = columnList
    # Dropping last column
    df2.drop(df2.columns[16], axis=1, inplace=True)

# Replacing all empty data with NaN
df2 = df2.replace(r'^\s*$', np.NaN, regex=True)

# Now deleting columns which has no values
filteredData = df2.dropna(how='all', axis='columns')
filteredData.head(5)
var = filteredData.shape
# print(filteredData.columns)
var1 = filteredData.dtypes

# Dropping duplicates values and save it as .csv in new folder saving with same name
filteredData = filteredData.drop_duplicates(keep=False)
os.chdir("C:/Users/kavin/Documents/LANInnovations/Cleansed_data/cleansedData")
filteredData.to_csv(filteredDataCSV, header=True, index=False)
# filteredData.dtypes

dfn = filteredData.convert_dtypes()
var2 = dfn.dtypes
dfn['Severity'].replace('', np.nan, inplace=True)
dfn['Severity'] = dfn['Severity'].astype('category')
# dfn["Severity"].describe

# Filter the column "Severity" with ERROR and Save as a csv file with the same name and append with _Error.
dfError = dfn[(dfn['Severity'] == 'ERROR')]
dfError.to_csv(errorCSV, header=True, index=False)

# Print Success Message
print(base + "  -->  Success")
