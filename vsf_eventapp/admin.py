
from django.contrib import admin
from .models import LimitFact, ActivityFact, LineDim, AccountDim, CalendarDate

admin.site.register(CalendarDate)
admin.site.register(AccountDim)
admin.site.register(LineDim)
admin.site.register(ActivityFact)
admin.site.register(LimitFact)
