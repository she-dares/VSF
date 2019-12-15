from django.shortcuts import render
from rest_framework.viewsets import ModelViewSet, ViewSet
from .serializers import ByLimitSerializer, ActivitySerializer
from .models import ActivityFact, LimitFact
from django.db.models import Min


class ActivityViewSet(ModelViewSet):
    queryset = ActivityFact.objects.all()
    serializer_class = ActivitySerializer


class ByActivity(ModelViewSet):
    def get_queryset(self):
        # qs1 = ActivityFact.objects.values('MTN').annotate(min_eventdt=Min('EVENT_DT')).filter('min_evendt' > 'MTN__SVC_ACT_DT').all()
        qs = ActivityFact.objects.filter(MTN__Device_Grouping="ANDROID")
        return qs

    serializer_class = ActivitySerializer


class ByLimit(ModelViewSet):
    def get_queryset(self):
        qs1 = ActivityFact.objects.values_list("MTN_id")
        qs2 = LimitFact.objects.filter(LIMIT_TYPE="USG").values_list("MTN_id")

        # out_queryset = (
        #     ActivityFact.objects.select_related('MTN_id__LIMIT_TYPE').filter(MTN_id__LIMIT_TYPE='USG')
        #     .values('MTN_id')
        #     .all()
        # )
        out_queryset = qs1.intersection(qs2).values("MTN").all()
        return out_queryset
        # return render(request, 'vsf_eventapp/mtn_listing.html', {'out_queryset': out_queryset })

    serializer_class = ByLimitSerializer


def render_aggregation(request):
    return render(request, "vsf_eventapp/index.html", {})
