from django.urls import include, path
from rest_framework.routers import DefaultRouter
from rest_framework.schemas import get_schema_view

from .views import ByLimit, ActivityViewSet, render_aggregation

router = DefaultRouter()

# Register some endpoints via "router.register(...)"

router.register("activity", ActivityViewSet, basename="activity")
router.register("by_limit", ByLimit, basename="by_limit")

schema_view = get_schema_view(title="VSF API")

urlpatterns = [
    path("api/", include(router.urls)),
    path("MTN_BY_LIMIT/", ByLimit, name="aggregation"),
    path("", render_aggregation, name="aggregation"),
]