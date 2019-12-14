from django.core.management import BaseCommand
from django.db import transaction
from ...models import LineDim, AccountDim, LimitFact, ActivityFact
import pandas as pd
import luigi
from luigi import build
import glob
import argparse
from vsf.djangotasks import LineDimVer, LineDimLoad, AccountDimLoad, LimitFactLoad
from vsf.tasks import ByMdn
from utils.luigi.dask.target import ParquetTarget

# Reference - https://stackoverflow.com/questions/20906474/import-multiple-csv-files-into-pandas-and-concatenate-into-one-dataframe


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser = parser.add_argument("-f", "--full", action="store_false", dest="full")

    def handle(self, *args, **options):
        build([LineDimLoad(subset=options["full"])], local_scheduler=True)
        df_LineDim = pd.read_parquet(
            "./data/linedim/part.0.parquet", engine="fastparquet"
        )

        build([AccountDimLoad(subset=options["full"])], local_scheduler=True)
        df_AccountDim = pd.read_parquet(
            "./data/accountdim/part.0.parquet", engine="fastparquet"
        )

        build([LimitFactLoad(subset=options["full"])], local_scheduler=True)
        df_LimitFact = pd.read_parquet(
            "./data/limitfact/part.0.parquet", engine="fastparquet"
        )

        build([ByMdn()], local_scheduler=True)
        local_root = "file://data/amplitude/by_mdn/"
        target = ParquetTarget(local_root, flag=False, glob="*.parquet")
        df_ActivityFact = target.read_dask(
            columns=["mdn", "event_time"], parse_dates=["event_time"]
        ).compute()

        LineDim.objects.all().delete()

        with transaction.atomic():
            df_LineDim_objs = [
                LineDim(
                    MTN=line["MTN"],
                    Device_Grouping=line["DEVICE_GROUPING"],
                    Sales_Channel=line["SALES_CHANNEL"],
                    SVC_ACT_DT=line["SVC_ACT_DT"],
                )
                for idx, line in df_LineDim.iterrows()
            ]
            LineDim.objects.bulk_create(df_LineDim_objs)

        AccountDim.objects.all().delete()
        with transaction.atomic():
            df_AcctDim_objs = [
                AccountDim(
                    Cust_Acct=acct["CUST_ACCT"],
                    Segment_Name=acct["SEGMENT_NAME"],
                    SVC_Plan=acct["SVC_PLAN"],
                )
                for idx, acct in df_AccountDim.iterrows()
            ]
            AccountDim.objects.bulk_create(df_AcctDim_objs)

        LimitFact.objects.all().delete()
        with transaction.atomic():
            df_LimitFact_objs = [
                LimitFact(
                    MTN=LineDim.objects.get_or_create(MTN=limit["MTN"])[0],
                    Cust_Acct=AccountDim.objects.get_or_create(
                        Cust_Acct=limit["CUST_ACCT"]
                    )[0],
                    LIMIT_DT=limit["LIMITING_DT"],
                    LIMIT_TYPE=limit["LIMIT_TYPE"],
                )
                for idx, limit in df_LimitFact.iterrows()
            ]
            LimitFact.objects.bulk_create(df_LimitFact_objs)

        ActivityFact.objects.all().delete()
        with transaction.atomic():
            df_ActivityFact_objs = [
                ActivityFact(
                    MTN=LineDim.objects.get_or_create(MTN=activity["mdn"])[0],
                    EVENT_DT=activity["event_time"],
                )
                for idx, activity in df_ActivityFact.iterrows()
            ]
            ActivityFact.objects.bulk_create(df_ActivityFact_objs)
