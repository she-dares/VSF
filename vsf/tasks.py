import os
import luigi
from luigi import ExternalTask, Parameter, Task
from luigi.contrib.s3 import S3Target, S3Client
from utils.luigi.dask.target import CSVTarget, ParquetTarget
from utils.luigi.task import TargetOutput

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

    S3_ROOT = "s3://cscie29vsf/amplitude/"  # Root S3 path, as a constant

    root = Parameter(default=S3_ROOT)

    def output(self):
        # return the S3Target of the files
        target = S3Target(self.S3_ROOT, format=luigi.format.Nop)
        return target

class ArchiveGzFile(ExternalTask):
    '''
    Requires - Output from VerifyFileArrived
    Run - Copy the File to Archive Directory
    Output - File Exists in the Archive Directory
    '''
    #copy(source_path, destination_path, threads=100, start_time=None, end_time=None, part_size=8388608, **kwargs)[
    #    source]
    #requires = Requires()
    S3_DEST_PATH = "s3://cscie29vsf/archive/"  # Destination S3 Path, as a constant, target directory
    client = S3Client()

    def requires(self):
        # Depends on the VerifyFileArrived ExternalTask being complete
        # i.e. the file must exist on S3 in order to take a backup and archive it
        return VerifyFileArrived()

    def output(self):
        # return the S3Target of the files
        return S3Target(self.S3_DEST_PATH, client=self.client)


    def run(self):
        # Use self.output() and self.input() targets to atomically copy
        # the file
        self.client.copy(self.input().path, self.output().path)

class SaveGzFileLocally(Task):
    '''
    Requires - Output from Archive Directory
    Run - Copy the File to Data Directory
    Output - File Exists in the Data Directory
    '''

    LOCAL_PATH = "data://cscie29vsf/amplitude/"  # Destination S3 Path, as a constant, target directory


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

