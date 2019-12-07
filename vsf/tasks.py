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


class OpenGzFile(ExternalTask):
    '''
    Requires - File from Data Directory (GZ Format)
    Run - Read the file to a Dataframe
    Output - Dataframe with all details
    '''


    # file_list = glob.glob('data/*.gz')
    file_list = '2019-05-20_testmdn_child_parent_rs.txt.gz'


    col_names = ['mdn', 'app', 'amplitude_id', 'device_id', 'user_id', 'event_time',
                 'client_event_time', 'client_upload_time', 'server_upload_time',
                 'event_id', 'session_id', 'event_type', 'amplitude_event_type',
                 'version_name', 'os_name', 'os_version', 'device_brand',
                 'device_manufacturer', 'device_model', 'device_carrier', 'country',
                 'language', 'location_lat', 'location_lng', 'ip_address',
                 'event_properties', 'user_properties', 'region', 'city', 'dma',
                 'device_family', 'device_type', 'platform', 'uuid', 'paying',
                 'start_version', 'user_creation_time', 'library', 'idfa', 'adid']


    output = TargetOutput(target_class=ParquetTarget, file_pattern= "file://data/by_decade/", ext="")



    # def requires(self):
    #     # Depends on the VerifyFileArrived ExternalTask being complete
    #     # i.e. the file must exist on S3 in order to take a backup and archive it
    #     return LocalGzFiles()


    def run(self):

        # delayed_dfs = [delayed(pd.read_csv)(f, delimiter='|') for f in self.file_list]
        delayed_dfs = delayed(pd.read_csv)(self.file_list, delimiter='|')
        ddf = dd.from_delayed(delayed_dfs)
        pandas_df = pd.DataFrame(columns=self.col_names + ['Event', 'Prop'])


        for key, value in ddf.iterrows():
            row_df = pd.DataFrame(data=[value.to_list()], columns=self.col_names)
            json_df = split_json(value['event_properties'])
            row_df['A'] = 1
            json_df['A'] = 1

            pandas_df = pd.concat([pandas_df, pd.merge(row_df, json_df, on='A').drop('A', 1)])
            pandas_df.columns = [col.encode('utf-8', 'replace').decode('utf-8') for col in pandas_df.columns]
            master_df = dd.from_pandas(pandas_df, npartitions=2)
        self.output().write_dask(master_df, compression="gzip", compute=True)
#
#




# class CleanandProcessData(Task):
#     '''
#     Requires - File from the Data Directory (txt format)
#     Run - Clean and Process the Files and convert to a csv/parquet file
#     Output - Parquet File with the salted output
#     '''
#     S3_ROOT = "https://s3.console.aws.amazon.com/s3/buckets/cscie29vsf/?region=us-east-1&tab=overview"
#
#     subset = BoolParameter(default=True)
#
#     requires = Requires()
#     activity_details = Requirement(ArchiveGzFile)
#
#     output = TargetOutput(target_class=ParquetTarget, file_pattern=LOCAL_ROOT, ext="")
#
#     def run(self):
#         numcols = ["funny", "cool", "useful", "stars"]
#         dtype_dic = {"funny": float, "cool": float, "useful": float, "stars": float}
#         dsk = self.input()["yelp_reviews"].read_dask(
#             parse_dates=["date"], dtype=dtype_dic
#         )
#
#         if self.subset:
#             dsk = dsk.get_partition(0)
#
#         out = (
#             dsk.dropna(subset=["user_id", "date"])[dsk["review_id"].str.len() == 22]
#                 .set_index("review_id")
#                 .fillna(value={col: 0.0 for col in numcols})
#                 .astype({col: "int32" for col in numcols})
#         )
#
#         self.output().write_dask(out, compression="gzip", compute=True)




'''
ToDo - Come up with a schema of what the tables should like
Then we create a django app called as Verizon Smart Family Application and create models analogous to the tables
Add commands to load the processed data from the parquet or csv and populate the tables
Create views, urls, for UI display 
Yesterday's lecture covered some nice visualization tools available, so we can make use of that
'''

