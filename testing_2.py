import os
import numpy as np
import pandas as pd
from pathlib import Path
from IPython import get_ipython

os.chdir("C:\\Users\\Kavin\\Documents\\SMP_testing\\Uncleaned_data")
directory1 = "C:\\Users\\Kavin\\Documents\\SMP_testing\\Uncleaned_data"
for folderName3 in os.listdir(directory1):
    UncleanedFolder = Path('C:/Users/Kavin/Documents/SMP_testing/Uncleaned_data')
    CleanedFolder = Path('C:/Users/Kavin/Documents/SMP_testing/Cleaned_data')
    subFolder = Path(folderName3)
    os.chdir(UncleanedFolder / subFolder)
    directory2 = UncleanedFolder / subFolder
    directory3 = CleanedFolder / subFolder
    print(os.getcwd())
    for filename in os.listdir(directory2):
        os.chdir(UncleanedFolder / subFolder)
        try:
            if filename.endswith(".log"):
                print(os.path.join(directory2, filename))
                base = os.path.splitext(filename)[0]
                os.rename(filename, base + ".txt")
                txtFileName = base + ".txt"
                txtFileTrimmed = base + "_LineTrimmed" + ".txt"
                colNames = base + "_Columns.txt"
                splitColNames = base + "_column1.txt"
                filteredDataCSV = base + ".csv"
                errorCSV = base + "_Error.csv"

                with open(txtFileName) as old, open(txtFileTrimmed, 'w') as new, open(colNames, 'w') as Col:
                    lines = old.readlines()
                    new.writelines(lines[11:])
                    print(new)
                    Col.writelines(lines[6])
                    print(Col)

                # Converting the Column text file into data table
                with open(colNames) as old, open(splitColNames, 'w') as col1:
                    df = pd.read_csv(colNames, header=None, sep=':')
                    lines1 = df[1]
                    df.head(5)
                    col1.writelines(lines1)

                # Separating the column into data table and list all column label available in dataframe
                df1 = pd.read_csv(splitColNames, header=None, sep='|')
                columnList = df1.values.tolist()[0]

                # Converting the text file into data table and separate it using pipe |
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
                elif len(df2.columns) == (len(columnList) + 1):
                    columnList.append('nan')
                    # Add Column Name to a dataframe
                    df2.columns = columnList
                    # Dropping last column
                    df2.drop(df2.columns[16], axis=1, inplace=True)
                elif len(df2.columns) == (len(columnList) + 2):
                    columnList.append('nan')
                    columnList.append('nan')
                    # Add Column Name to a dataframe
                    df2.columns = columnList
                    # Dropping last column
                    df2.drop(df2.columns[16], axis=1, inplace=True)
                elif len(df2.columns) == (len(columnList) + 3):
                    columnList.append('nan')
                    columnList.append('nan')
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
                os.chdir(directory3)
                print(os.getcwd())
                filteredData.to_csv(filteredDataCSV, header=True, index=False)
                # Convert columns to best possible dtypes using dtypes supporting ``pd.NA``
                dfn = filteredData.convert_dtypes()
                var2 = dfn.dtypes
                # Again if there is any empty space in Severity series then replace it with nan
                dfn['Severity'].replace('', np.nan, inplace=True)
                dfn['Severity'] = dfn['Severity'].astype('category')
                # Describe Filter the column "Severity" with ERROR and Save as a csv file with the
                # same name and append with _Error.
                dfError = dfn[(dfn['Severity'] == 'ERROR')]
                dfError.to_csv(errorCSV, header=True, index=False)

                # Print Success Message
                print(base + "  -->  Success")

                # Clear all variables
                # sys.modules[__name__].__dict__.clear()
                get_ipython().magic('reset -sf')
            else:
                continue
        except Exception:
            # Print Success Message
            print("Failed")
            pass
