import os
import luigi
import boto3
import pandas as pd
import dask.dataframe as dd
from dask import delayed
from luigi import ExternalTask, Parameter, Task, LocalTarget
from luigi.contrib.s3 import S3Target, S3Client
from utils.luigi.dask.target import CSVTarget, ParquetTarget
from utils.luigi.task import TargetOutput
from utils.luigi.task import Requires, Requirement


"""
Create a  Luigi task to verify that the file for the processing date has arrived and available for processing.
Create a Luigi task to take the backup up of .gz file and move it to the archive directory at the Local Server. 
Create a Luigi task to gunzip and save the .gz file locally
Create the Luigi task to copy the file from Local to HDFS directory.
Create the Luigi External Task to invoke the script for data processing.
Create the Luigi Task to move data from Hive to SQLLite Relational Database
Generate the ORM model and classes for data 
APIs and the authentication mechanisms
Build UI App for displaying the data 
"""


class VerifyFileArrived(ExternalTask):

    """
    This should check if there are any files for processing for today
    So we have a directory called as amplitude and if there are files present, we go ahead with the processing
    We create a TargetOutput(), which returns the target file path for processing
    Output - Returns the S3 target path
    """

    S3_ROOT = "s3://cscie29vsf/amplitude/"  # Root S3 path, as a constant

    root = Parameter(default=S3_ROOT)

    def check_s3_files_exists(self):
        client = boto3.client("s3")
        # see if files exists
        object_listing = client.list_objects_v2(
            Bucket="cscie29vsf", Prefix="amplitude/"
        )
        obj_list = object_listing["Contents"]
        return True if len(obj_list) > 1 else False

    def output(self):
        # return the S3Target of the files
        target = CSVTarget(self.S3_ROOT, flag=False, glob="*.*")
        if self.check_s3_files_exists():
            return target


class ArchiveGzFile(ExternalTask):
    """
    Requires - Output from VerifyFileArrived
    Run - Copy the File to Archive Directory
    Output - File Exists in the Archive Directory
    """

    S3_DEST_PATH = (
        "s3://cscie29vsf/archive/"
    )  # Destination S3 Path, as a constant, target directory
    client = S3Client()

    requires = Requires()
    verify_files = Requirement(VerifyFileArrived)

    def output(self):
        # return the S3Target of the files
        return S3Target(self.S3_DEST_PATH, client=self.client)

    def run(self):
        # Use self.output() and self.input() targets to atomically copy
        # the file
        self.client.copy(self.input().path, self.output().path)


class CleanandProcessData(Task):
    """
    Requires - File from the Data Directory (txt format)
    Run - Clean and Process the Files and convert to a csv/parquet file
    Output - Parquet File with the salted output
    """

    LOCAL_ROOT = "file://data/amplitude/"

    col_names = [
        "mdn",
        "app",
        "amplitude_id",
        "device_id",
        "user_id",
        "event_time",
        "client_event_time",
        "client_upload_time",
        "server_upload_time",
        "event_id",
        "session_id",
        "event_type",
        "amplitude_event_type",
        "version_name",
        "os_name",
        "os_version",
        "device_brand",
        "device_manufacturer",
        "device_model",
        "device_carrier",
        "country",
        "language",
        "location_lat",
        "location_lng",
        "ip_address",
        "event_properties",
        "user_properties",
        "region",
        "city",
        "dma",
        "device_family",
        "device_type",
        "platform",
        "uuid",
        "paying",
        "start_version",
        "user_creation_time",
        "library",
        "idfa",
        "adid",
    ]

    requires = Requires()
    verify_files = Requirement(VerifyFileArrived)
    archive_files = Requirement(ArchiveGzFile)

    output = TargetOutput(target_class=ParquetTarget, file_pattern=LOCAL_ROOT, ext="")

    def run(self):
        dtype_dic = {"amplitude_id": "object", "os_version": "object", "mdn": "object"}
        dsk = self.input()["verify_files"].read_dask(
            parse_dates=[
                "event_time",
                "client_event_time",
                "client_upload_time",
                "server_upload_time",
            ],
            dtype=dtype_dic,
            delimiter="|",
            compression="gzip",
            blocksize=None,
        )
        self.output().write_dask(dsk, compression="gzip", compute=True)


class ByMdn(Task):
    LOCAL_ROOT = "file://data/amplitude/by_mdn/"

    requires = Requires()
    clean_files = Requirement(CleanandProcessData)

    output = TargetOutput(target_class=ParquetTarget, file_pattern=LOCAL_ROOT, ext="")

    def print_results(self):
        print(self.output().read_dask().compute())

    def run(self):
        dsk = self.input()["clean_files"].read_dask(
            columns=["mdn", "event_time"], parse_dates=["event_time"]
        )

        out = dsk.dropna()

        self.output().write_dask(out, compression="gzip")
        self.print_results()


"""
ToDo - Come up with a schema of what the tables should like
Then we create a django app called as Verizon Smart Family Application and create models analogous to the tables
Add commands to load the processed data from the parquet or csv and populate the tables
Create views, urls, for UI display 
Yesterday's lecture covered some nice visualization tools available, so we can make use of that
"""
