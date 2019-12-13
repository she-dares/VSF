from django.shortcuts import render
from rest_framework.viewsets import ModelViewSet, ViewSet
from .serializers import ByLimitSerializer, ActivitySerializer
from .models import LineDim, ActivityFact, LimitFact



class ActivityViewSet(ModelViewSet):
    queryset = ActivityFact.objects.all()
    serializer_class = ActivitySerializer



class ByLimit(ModelViewSet):
    def get_queryset(self):
        # print("Hello World")

        out_queryset = (
            ActivityFact.objects.select_related('MTN_id__LIMIT_TYPE').filter(MTN_id__LIMIT_TYPE='USG')
            .values('MTN_id')
            .all()
        )
        print(out_queryset)

        return out_queryset

    serializer_class = ByLimitSerializer

def render_aggregation(request):
    return render(request, "vsf_eventapp/index.html", {})