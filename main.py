import os
import re
import numpy as np
import pandas as pd
from pathlib import Path
# from zipfile import ZipFile
from IPython import get_ipython

# ========================================Folder creation Block======================####
os.chdir("C:\\Users\\Kavin\\Documents\\SMP_testing")
print(os.getcwd())
directory = "C:\\Users\\Kavin\\Documents\\SMP_testing"
folderName = []
for folderNames in os.listdir(directory):
    if folderNames.endswith(".zip"):
        folderName.append(folderNames)
print(folderName)
os.mkdir("Cleaned_data")
os.chdir("C:\\Users\\Kavin\\Documents\\SMP_testing\\Cleaned_data")
for folderName1 in folderName:
    only_folderName = folderName1[:-4]
    os.mkdir(only_folderName)

"""
# ======================================Files extraction Block=====================####
os.chdir("C:\\Users\\Kavin\\Documents\\SMP")
for folderName2 in os.listdir(directory):
    os.chdir("C:\\Users\\Kavin\\Documents\\SMP")
    if folderName2.endswith(".zip"):
        print(folderName2)
        with ZipFile(folderName2, 'r') as Zip:
            # printing all the contents of the zip file
            Zip.printdir()
            # extracting all the files
            print('Extracting all the files now...')
            homeFolder = Path('C:/Users/Kavin/Documents/SMP/Uncleaned_data')
            subFolder = Path(folderName2[:-4])
            os.chdir(homeFolder / subFolder)
            print(os.getcwd())
            Zip.extractall()
            print('Done!')
"""

# ====================================Data Mining Block=======================####
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

# Set the current working directory for merging all csv files
os.chdir("C:\\Users\\Kavin\\Documents\\SMP_testing\\Cleaned_data")
for folderName4 in os.listdir(directory1):
    CleanedFolder = Path('C:/Users/Kavin/Documents/SMP_testing/Cleaned_data')
    subFolder = Path(folderName4)
    os.chdir(CleanedFolder / subFolder)
    directory5 = CleanedFolder / subFolder
    print(os.getcwd())
    # List all the cleansed data contains with only ERROR into a list using RegEx
    regex = re.compile('(.*_Error.csv$)')
    fileList = []
    for root, dirs, files in os.walk(directory5):
        for file in files:
            if regex.match(file):
                print(file)
                fileList.append(file)
    # combine all files in the list
    combined_csv = pd.concat([pd.read_csv(f) for f in fileList])
    # Drop duplicate values present in Text series
    combined_csv.drop_duplicates(subset=['Text'], keep=False)
    # export to csv
    combined_csv.to_csv("Chatbot_Data.csv", index=False, encoding='utf-8-sig')
    print("Successfully concatenated all Error.csv files into a Chatbot_data.csv file")
