from django.db import models
from django.contrib.auth.models import AbstractUser, BaseUserManager
from django.utils.translation import gettext_lazy as _


# Create your models here.
class outputs(models.Model):
    DOI = models.CharField(max_length=30, db_column='DOI', unique=True)
    id = models.CharField(max_length=30, db_column='_id', unique=True, primary_key=True)
    publisher=models.CharField(max_length=150, db_column='publisher')
    class Meta:
        managed = True
        db_table = 'outputs'


class CustomUserManager(BaseUserManager):
    def _create_user(self,email,password=None, **extra_fields):
        if not email:
            raise ValueError('The given email must be set')
        email = self.normalize_email(email)
        user=self.model(email=email, **extra_fields)
        user.set_password(password)
        user.save(using=self._db)
        return user

    def create_user(self,email,password=None,**extra_fields):
        extra_fields.setdefault('is_staff',False)
        extra_fields.setdefault('is_superuser',False)
        return self._create_user(email, password, **extra_fields)

    def create_superuser(self,email,password=None,**extra_fields):
        extra_fields.setdefault('is_staff',True)
        extra_fields.setdefault('is_superuser',True)
        
        if extra_fields.get('is_staff') is not True:
            raise ValueError('Superuser must have is_staff=True')
        
        if extra_fields.get('is_superuser') is not True:
            raise ValueError('Superuser must have is_superuser=True')

        return self._create_user(email,password,**extra_fields)
        



class User(AbstractUser):
    name = models.CharField('name', max_length=100)
    email = models.EmailField('email',max_length=100, unique=True)
    password = models.CharField(max_length=100)
    member_id = models.IntegerField('member_id', null=True)
    username = None
    USERNAME_FIELD = 'email'
    REQUIRED_FIELDS = []

    object = CustomUserManager()
