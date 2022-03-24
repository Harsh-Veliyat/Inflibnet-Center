from django.urls import path
from .views import DataByDoi, GetDataByOrcid, GetData, GetDataForReact

urlpatterns = [
    path('getData', GetData.as_view()),
    path('DataByDoi/<path:doi>', DataByDoi.as_view()),
    path('getDataByOrcid/<path:orcid>', GetDataByOrcid.as_view()),
    path('', GetDataForReact.as_view(), name='reactData'),
    #path('getDataByAffil/<path:affilliation>', GetDataByAffiliation.as_view()),
    #path('getDataByROR/<path:ror>', GetDataByOrcid.as_view())
]
