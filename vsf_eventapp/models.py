from django.db import models

#This table stores the generic calendar table data for rollups and aggregation
class CalendarDate(models.Model):
    Calendar_Dt = models.DateField(primary_key=True)   # Date field
    Month_Nbr = models.IntegerField()
    Month_Year = models.IntegerField()
#    def __str__(self): return self.date

#This table stores that account level dimension data
class AccountDim(models.Model):
    Cust_Acct = models.CharField(max_length=50,primary_key=True)
    Segment_Name = models.CharField(max_length=50)
    SVC_Plan = models.CharField(max_length=50)

#This table stores the line level dimension data
class LineDim(models.Model):
    MTN = models.CharField(max_length=50,primary_key=True)
    Device_Grouping = models.CharField(max_length=50)
    Sales_Channel = models.CharField(max_length=50)

#This table corresponds to the Limit Fact data and stores the USage Limit data
class LimitFact(models.Model):
    MTN = models.ForeignKey(LineDim, on_delete=models.CASCADE) # ForeignKey
    Cust_Acct = models.ForeignKey(AccountDim, on_delete=models.CASCADE) # ForeignKey
    LIMIT_DT = models.DateField(max_length=50)
    LIMIT_TYPE= models.CharField(max_length=50)
