"""ESS URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, include

from drf_yasg.views import get_schema_view
from drf_yasg import openapi

from ESS_App.views import GetDataForReact, LoginView, LogoutView

schema_view = get_schema_view(
   openapi.Info(
      title="User Management System",
      default_version='v1',
      description="Basic User Management Service",
   ),
   public=True,
   #url=BASE_URL,
)

urlpatterns = [
    path('', schema_view.with_ui('swagger', cache_timeout=0), name='schema-swagger-ui'),
    path('admin/', admin.site.urls),
    path('getData/', include('ESS_App.urls')),
    path('react/<int:limit>', GetDataForReact.as_view()),
    path('login', LoginView.as_view()),
    path('logout', LogoutView.as_view()),
]
