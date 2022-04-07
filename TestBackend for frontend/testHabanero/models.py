from django.db import models

# Create your models here.
class outputs(models.Model):
    indexed = models.JSONField('indexed')
    class Meta:
        managed = True
        db_table = 'outputs'
