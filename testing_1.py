import os
# import numpy as np
# import pandas as pd
from pathlib import Path
from zipfile import ZipFile
# from IPython import get_ipython
# import re

myFiles = ["main.py", "file0.py", "testing_1.py"]
for file in myFiles:
    print(str(Path(r'C:\Users\AI', file)))

print(Path.home())

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
os.chdir("C:\\Users\\Kavin\\Documents\\SMP_testing")
os.mkdir("Uncleaned_data")
os.chdir("C:\\Users\\Kavin\\Documents\\SMP_testing\\Uncleaned_data")
for folderName1 in folderName:
    only_folderName = folderName1[:-4]
    print(only_folderName)
    os.mkdir(only_folderName)

os.chdir("C:\\Users\\Kavin\\Documents\\SMP_testing")
for folderName2 in os.listdir(directory):
    os.chdir("C:\\Users\\Kavin\\Documents\\SMP_testing")
    if folderName2.endswith(".zip"):
        print(folderName2)
        with ZipFile(folderName2, 'r') as Zip:
            # printing all the contents of the zip file
            Zip.printdir()
            # extracting all the files
            print('Extracting all the files now...')
            homeFolder = Path('C:/Users/Kavin/Documents/SMP_testing/Uncleaned_data')
            subFolder = Path(folderName2[:-4])
            os.chdir(homeFolder / subFolder)
            print(os.getcwd())
            Zip.extractall()
            print('Done!')
