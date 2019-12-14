from utils.luigi.dask.target import CSVTarget, ParquetTarget
import dask.dataframe as dd
from luigi import ExternalTask, Parameter, Task, BoolParameter
from utils.luigi.dask import *
from utils.luigi.task import Requires, Requirement, TargetOutput


class LineDimVer(ExternalTask):
    S3_ROOT = "s3://groupprojectcscie29/LineDimension/"
    """
    This class verifies that the target exists for the processing. This task loads the Line Dimension data.
    """

    def output(self):
        target = CSVTarget(self.S3_ROOT, flag=False, glob="*.csv")
        return target


class LineDimLoad(Task):
    LOCAL_ROOT = "./data/linedim/"
    """
    This task outputs a local ParquetTarget in ./data/linedim/ for further load into the database.
    """

    subset = BoolParameter(default=True)
    requires = Requires()
    line_dim = Requirement(LineDimVer)
    output = TargetOutput(target_class=ParquetTarget, file_pattern=LOCAL_ROOT, ext="")

    def run(self):
        dtype_dic = {
            "MDN": "object",
            "DEVICE_GROUPING": "object",
            "SALES_CHANNEL": "object",
        }
        dsk = self.input()["line_dim"].read_dask(dtype=dtype_dic)

        if self.subset:
            dsk = dsk.get_partition(0)

        self.output().write_dask(dsk, compression="gzip", compute=True)


class AccountDimVer(ExternalTask):
    S3_ROOT = "s3://groupprojectcscie29/AccountDimension/"
    """
    This class verifies that the target exists for the processing. This task loads the Account Dimension data.
    """

    def output(self):
        target = CSVTarget(self.S3_ROOT, flag=False, glob="*.csv")
        return target


class AccountDimLoad(Task):
    LOCAL_ROOT = "./data/accountdim/"
    """
    This task outputs a local ParquetTarget in ./data/accountdim/ for further load into the database.
    """

    subset = BoolParameter(default=True)
    requires = Requires()
    account_dim = Requirement(AccountDimVer)
    output = TargetOutput(target_class=ParquetTarget, file_pattern=LOCAL_ROOT, ext="")

    def run(self):
        dtype_dic = {
            "CUST_ACCT": "object",
            "SEGMENT_NAME": "object",
            "SVC_PLAN": "object",
        }
        dsk = self.input()["account_dim"].read_dask(dtype=dtype_dic)

        if self.subset:
            dsk = dsk.get_partition(0)

        self.output().write_dask(dsk, compression="gzip", compute=True)


class LimitFactVer(ExternalTask):
    S3_ROOT = "s3://groupprojectcscie29/LimitFact/"
    """
    This class verifies that the target exists for the processing. This task loads the Limit Facts data.
    """

    def output(self):
        target = CSVTarget(self.S3_ROOT, flag=False, glob="*.csv")
        return target


class LimitFactLoad(Task):
    LOCAL_ROOT = "./data/limitfact/"
    """
    This task outputs a local ParquetTarget in ./data/limitfact/ for further load into the database.
    """

    subset = BoolParameter(default=True)
    requires = Requires()
    limit_fact = Requirement(LimitFactVer)
    output = TargetOutput(target_class=ParquetTarget, file_pattern=LOCAL_ROOT, ext="")

    def run(self):
        dtype_dic = {
            "MTN": "object",
            "CUST_ACCT": "object",
            "LIMITING_DT": "object",
            "LIMIT_TYPE": "object",
        }
        dsk = self.input()["limit_fact"].read_dask(dtype=dtype_dic)

        if self.subset:
            dsk = dsk.get_partition(0)

        self.output().write_dask(dsk, compression="gzip", compute=True)
