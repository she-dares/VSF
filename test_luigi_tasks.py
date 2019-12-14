import os
import pandas as pd
from unittest import TestCase
from unittest.mock import patch, MagicMock
from luigi import build, ExternalTask
from luigi.execution_summary import LuigiStatusCode
from tempfile import TemporaryDirectory

from utils.luigi.dask.target import ParquetTarget, CSVTarget
from luigi.contrib.s3 import S3Target
from utils.luigi.task import TargetOutput, Requirement, Requires
from vsf.tasks import VerifyFileArrived, ArchiveGzFile, CleanandProcessData, ByMdn
from vsf.djangotasks import LineDimVer, LineDimLoad, AccountDimVer, AccountDimLoad, LimitFactVer, LimitFactLoad
from luigi import build, ExternalTask, format, Parameter, Task, BoolParameter
from dask import dataframe as dd

MOCK_VALUES = [True, False]
SUBSET = [True, False]
RESULTS = [LuigiStatusCode.SUCCESS, LuigiStatusCode.MISSING_EXT]

d = {
    "mdn": [
        "6777777776",
        "7167167161",
        "3333333333",
    ],
    "event_time": [
        "2019-05-20",
        "2019-07-01",
        "2019-08-02",
    ],
    "client_event_time": [
        "2019-05-20",
        "2019-07-01",
        "2019-08-02",
    ],
    "client_upload_time" : [
        "2019-05-20",
        "2019-07-01",
        "2019-08-02",
    ],
    "server_upload_time" : [
        "2019-05-20",
        "2019-07-01",
        "2019-08-02",
    ],

}
df = pd.DataFrame(d)


def build_func(func, **kwargs):
    return build([func(**kwargs)], local_scheduler=True, detailed_summary=True).status


csv_target_exists = "utils.luigi.dask.target.CSVTarget.exists"
parquet_target_exists = "utils.luigi.dask.target.ParquetTarget.exists"
s3_target_exists = "luigi.contrib.s3.S3Target.exists"


class TasksDataTests(TestCase):
    def test_FilesExistsS3(self):
        """Ensure VerifyFileArrived, ArchiveGz, LineDimVer, AccountDimVer, LimitFactVer works correctly and as expected"""

        for mock_value, result in zip(MOCK_VALUES, RESULTS):
            with patch(csv_target_exists, MagicMock(return_value=mock_value)):
                self.assertEqual(build_func(VerifyFileArrived), result)
                self.assertEqual(build_func(LineDimVer), result)
                self.assertEqual(build_func(AccountDimVer), result)
                self.assertEqual(build_func(LimitFactVer), result)
                CSVTarget.exists.assert_called()

        self.assertEqual(
            build([ArchiveGzFile()], local_scheduler=True, detailed_summary=True).status,
            LuigiStatusCode.SUCCESS,
        )

    def test_LuigiTasksForDjango(self):
        """Ensure all the luigi tasks works correctly and as expected"""

        for mock_value, result in zip(MOCK_VALUES, RESULTS):
            with patch(csv_target_exists, MagicMock(return_value=False)):
                with patch(
                        parquet_target_exists, MagicMock(return_value=mock_value)
                ):
                    self.assertEqual(
                        build_func(CleanandProcessData), result
                    )
                    self.assertEqual(build_func(ByMdn), result)
                    self.assertEqual(build_func(LineDimLoad), result)
                    self.assertEqual(build_func(AccountDimLoad), result)
                    self.assertEqual(build_func(LimitFactLoad), result)

                    ParquetTarget.exists.assert_called()

    def test_CleanandProcess_Run(self):
        """Test run method of CleanandProcess"""

        with TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "parent.txt.gz")
            df.to_csv(path, index=False, sep="|", compression="gzip")

            assert os.path.exists(path)

            path = tmp + "/"

            class NewAmplitudeFiles(ExternalTask):
                output = TargetOutput(
                    target_class=CSVTarget,
                    file_pattern=path,
                    ext="",
                    flag=False,
                    glob="*.gz",
                )

            self.assertEqual(build_func(NewAmplitudeFiles), LuigiStatusCode.SUCCESS)

            class NewCleanedData(CleanandProcessData):
                verify_files = Requirement(NewAmplitudeFiles)
                output = TargetOutput(
                    target_class=ParquetTarget, file_pattern=path, ext=""
                )

            self.assertEqual(build_func(NewCleanedData), LuigiStatusCode.SUCCESS)

            class NewByMdn(ByMdn):
                LOCAL_ROOT = path + "mdn"
                clean_files = Requirement(NewCleanedData)
                output = TargetOutput(
                    file_pattern=LOCAL_ROOT, target_class=ParquetTarget, ext=""
                )

            self.assertEqual(build_func(ByMdn), LuigiStatusCode.SUCCESS)

    def test_DjangoTasksProcess_Run(self):
        self.assertEqual(build_func(LineDimLoad), LuigiStatusCode.SUCCESS)
        self.assertEqual(build_func(AccountDimLoad), LuigiStatusCode.SUCCESS)
        self.assertEqual(build_func(LimitFactLoad), LuigiStatusCode.SUCCESS)


def create_dask_dataframe():
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    dask_df = dd.from_pandas(df, npartitions=2)
    return dask_df


def read_dask_dataframe(dsk):
    assert len(dsk.columns) == 2
    assert len(dsk.index) == 3


class DaskTests(TestCase):
    """Testing TargetOutput, Requires, Requirement, CSVTarget, ParquetTarget"""

    def test_tasks(self):
        with TemporaryDirectory() as tmp:
            fp1 = os.path.join(tmp, "data/parquet/")
            fp2 = os.path.join(tmp, "data/csv/")

            class MyTaskA(Task):
                output = TargetOutput(
                    target_class=ParquetTarget, file_pattern=fp1, ext=""
                )

                def run(self):
                    # create dataframe with some data
                    dask_df = create_dask_dataframe()
                    self.output().write_dask(dask_df, compression="gzip", compute=True)

            class MyTaskB(Task):
                output = TargetOutput(target_class=CSVTarget, file_pattern=fp2, ext="")

                def run(self):
                    # create dataframe with some data
                    dask_df = create_dask_dataframe()
                    self.output().write_dask(dask_df, compute=True)

            class MyTaskC(Task):

                requires = Requires()
                a_reviews = Requirement(MyTaskA)
                b_reviews = Requirement(MyTaskB)

                def run(self):

                    dsk = self.input()["a_reviews"].read_dask(columns=["a", "b"])

                    read_dask_dataframe(dsk)

        self.assertEqual(
            build([MyTaskC()], local_scheduler=True, detailed_summary=True).status,
            LuigiStatusCode.SUCCESS,
        )

