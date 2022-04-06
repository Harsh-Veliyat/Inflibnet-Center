from django.urls import path
from .views import DataByDoi, GetDataByOrcid, GetData, GetDataForReact, GetDataByAffiliation, LoginView, LogoutView

urlpatterns=[
     path('', GetData.as_view()),
    path('DataByDoi/<path:doi>', DataByDoi.as_view()),
    path('getDataByOrcid/<path:orcid>', GetDataByOrcid.as_view()),
    path('getDataByAffiliation/<str:affil>', GetDataByAffiliation.as_view()),
    # path('<int:limit>', GetDataForReact.as_view(), name='reactData'),
]