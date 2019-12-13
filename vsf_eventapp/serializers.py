from rest_framework.serializers import ModelSerializer, Serializer
from .models import ActivityFact

# from django.db import models
from rest_framework.serializers import FloatField, IntegerField

class ActivitySerializer(ModelSerializer):
    class Meta:
        model = ActivityFact
        fields = "__all__"

class ByLimitSerializer(Serializer):
    # Return the *averages*, not the sum!
    year = IntegerField(default=0)
    tot_count = IntegerField(default=0)
    stars_avg = FloatField(default=0.0)
    useful_avg = FloatField(default=0.0)
    funny_avg = FloatField(default=0.0)
    cool_avg = FloatField(default=0.0)

    class Meta:
        # model = ActivityFact
        fields = "__all__"