from django.shortcuts import render
from rest_framework.viewsets import ModelViewSet, ViewSet
from .models import LineDim, ActivityFact

# Create your views here.
class ByActivityDays(ModelViewSet):
    def get_queryset(self):

        out_queryset = (
            ActivityFact.objects.annotate(year=F("date_id") / 10000)
            .values("MTN_id")
            .annotate(
                delta_days = LineDim.objects.get(pk="MTN_id").
                tot_count=Sum("count"),
                stars_avg=ExpressionWrapper(
                    Sum("stars") * 1.0 / Sum("count") * 1.0, output_field=FloatField()
                ),
                useful_avg=ExpressionWrapper(
                    Sum("useful") * 1.0 / Sum("count") * 1.0, output_field=FloatField()
                ),
                funny_avg=ExpressionWrapper(
                    Sum("funny") * 1.0 / Sum("count") * 1.0, output_field=FloatField()
                ),
                cool_avg=ExpressionWrapper(
                    Sum("cool") * 1.0 / Sum("count") * 1.0, output_field=FloatField()
                ),
            )
            .order_by("year")
            .distinct()
            .all()
        )

        return out_queryset

    serializer_class = ByYearSerializer