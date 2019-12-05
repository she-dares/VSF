import os
import luigi
from luigi import ExternalTask, Parameter, Task
#from luigi.contrib.s3 import S3Target

'''
Create a  Luigi task to verify that the file for the processing date has arrived and available for processing.
Create a Luigi task to take the backup up of .gz file and move it to the archive directory at the Local Server. 
Create a Luigi task to gunzip and save the .gz file locally
Create the Luigi task to copy the file from Local to HDFS directory.
Create the Luigi External Task to invoke the script for data processing.
Create the Luigi Task to move data from Hive to SQLLite Relational Database
Generate the ORM model and classes for data 
APIs and the authentication mechanisms
Build UI App for displaying the data 
'''
class VerifyFileArrived(ExternalTask):
    '''
    This should check if there are any files for processing for today
    So we have a directory called as current and if files are present for that date, we go ahead with the processing
    Here we need to create a TargetOutput(), which checks files exists for processing
    '''
    pass

class ArchiveGzFile(ExternalTask):
    '''
    Requires - Output from VerifyFileArrived
    Run - Copy the File to Archive Directory
    Output - File Exists in the Archive Directory
    '''
    pass

class SaveGzFileLocally(Task):
    '''
    Requires - Output from Archive Directory
    Run - Copy the File to Data Directory
    Output - File Exists in the Data Directory
    '''
    pass

class CleanandProcessData(Task):
    '''
    Requires - File from the Data Directory (txt format)
    Run - Clean and Process the Files and convert to a csv/parquet file
    Output - Parquet File with the salted output
    '''
    pass

'''
ToDo - Come up with a schema of what the tables should like
Then we create a django app called as Verizon Smart Family Application and create models analogous to the tables
Add commands to load the processed data from the parquet or csv and populate the tables
Create views, urls, for UI display 
Yesterday's lecture covered some nice visualization tools available, so we can make use of that
'''

