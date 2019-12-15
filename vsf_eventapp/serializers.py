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
    MTN = IntegerField()


    class Meta:
        # model = ActivityFact
        fields = "__all__"