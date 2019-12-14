#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
from django.test import TestCase as DJTest
from unittest import TestCase
from vsf_eventapp.models import LineDim, ActivityFact, LimitFact, AccountDim


class APIViewTests(DJTest):
    def setUp(self):

        dates = [datetime.date(y, 1, 1) for y in range(2010, 2019)]
        mtn_id = 1000
        act_id = 2000

        activityfactobj = []
        limitfactobj = []
        for d in dates:
            activityfactobj.append(
                ActivityFact.objects.get_or_create(
                    MTN=LineDim.objects.get_or_create(MTN=mtn_id)[0], EVENT_DT=d
                )
            )
            limitfactobj.append(
                LimitFact.objects.get_or_create(
                    MTN=LineDim.objects.get_or_create(MTN=mtn_id)[0],
                    LIMIT_DT=d,
                    LIMIT_TYPE="USG",
                    Cust_Acct=AccountDim.objects.get_or_create(Cust_Acct=act_id)[0],
                )
            )
            mtn_id = mtn_id + 1
            act_id = act_id + 1

    def test_activity_fact(self):
        self.assertEqual(LineDim.objects.count(), 9)
        self.assertEqual(ActivityFact.objects.count(), 9)
        self.assertEqual(AccountDim.objects.count(), 9)
