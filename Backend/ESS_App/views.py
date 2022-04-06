#imports from local
# from .models import outputs
# from .serializers import outputsSerializer

# #import from packages
from tabnanny import check
from pymongo import MongoClient, errors
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated, IsAdminUser
from rest_framework.exceptions import AuthenticationFailed
from bson import ObjectId
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
import jwt, datetime

from django.contrib.auth.hashers import check_password
from django.contrib.auth import authenticate, login

from ESS.settings import CLIENT
import urllib
import json


db = CLIENT['crossrefTest']
collection_name = db['outputs']
check_user = db['ESS_App_user']

userField=db['RoleBasedFields']
query = [
    {
        '$match': {}
    }, {
        '$skip': 1
    }, {
        '$limit': 3
    }, {
        '$facet': {
            'Journal': [
                {
                    '$group': {
                        '_id': '$container-title', 
                        'Publication Count': {
                            '$sum': 1
                        }, 
                        'Citation Count': {
                            '$sum': '$is-referenced-by-count'
                        }
                    }
                }
            ], 
            'Subject': [
                {
                    '$unwind': '$subject'
                }, {
                    '$project': {
                        'is-referenced-by-count': 1, 
                        'subject': 1, 
                        '_id': 0
                    }
                }, {
                    '$group': {
                        '_id': '$subject', 
                        'Publication Count': {
                            '$count': {}
                        }, 
                        'Citation Count': {
                            '$sum': '$is-referenced-by-count'
                        }
                    }
                }
            ], 
            'PublishYear': [
                {
                    '$group': {
                        '_id': '$publishYear', 
                        'Publication Count': {
                            '$sum': 1
                        }, 
                        'Citation Count': {
                            '$sum': '$is-referenced-by-count'
                        }
                    }
                }
            ], 
            'publication count': [
                {
                    '$count': 'count'
                }
            ], 
            'citation count': [
                {
                    '$group': {
                        '_id': '$null', 
                        'count': {
                            '$sum': '$is-referenced-by-count'
                        }
                    }
                }
            ]
        }
    }
]


class GetDataForReact(APIView):
    def post(self,request, limit):
        token = request.COOKIES.get('jwt')
        if not token:
            raise AuthenticationFailed('Unauthenticated, please login')
        
        try:
                payload = jwt.decode(token, 'secret', algorithms=['HS256'])
        except jwt.ExpiredSignatureError:
            raise AuthenticationFailed('Token expired, login again!')
        data =  collection_name.find({},{"title":1 ,"DOI":1, "author":1}).limit(limit)
        dataList = []
        for d in data:
            d['_id'] = str(d['_id'])
            dataList.append(d)
        if len(dataList) != 0:
            return Response(dataList)
        else:
            return Response({'statuss':'Error'})


class GetData(APIView):
    permission_classes=[IsAuthenticated]
    page=openapi.Parameter('page', openapi.IN_QUERY, description='Numeric field, works as skip functionality: page 2 count 5 will skip first 5 result and show next five.', type=openapi.TYPE_INTEGER, default=1,required=True)
    count=openapi.Parameter('count', openapi.IN_QUERY, description='Numeric field, works as number of result per execution. count 5 will give 5 result. Minimum 1', type=openapi.TYPE_NUMBER, required=True)
    filter=openapi.Parameter('filter', openapi.IN_QUERY, description='Search Filter. id: ObjectID, publisher:publisher keywords, doi:DOI, title: title keywords, author:author keywords', type=openapi.TYPE_OBJECT, default={
        'publisher':'null',
        'doi':'null',
        'title':'null',
        'author':'null'
        })
    projectFilter=openapi.Parameter('projectFilter', openapi.IN_QUERY, description='Project Filter, Enter {field name:1} to those you want to see', type=openapi.TYPE_OBJECT)
    @swagger_auto_schema(manual_parameters=[page,count,filter, projectFilter])
    def post(self, request):
        if request.user.is_superuser:
            userRole = 'superAdmin'
        elif request.user.is_staff:
            userRole = 'admin'
        else:
            userRole = 'user'
        proj = userField.find({'role':userRole},{'project':1,'_id':0})
        project={} 
        for proj in proj:
            project = proj['project']
        print(list(project.keys()))
        
        limit = int(request.GET.get('count'))
        if (limit < 1):
            return Response({'status':'Error. Count should be minimum 1'})
        skip=(int(request.GET.get('page'))-1)*limit
        if (skip < 0):
            return Response({'status':'Error. Page should be minimum 1'})
        filter = json.loads(request.GET.get('filter'))
        projectFilter=json.loads(request.GET.get('projectFilter'))

        if projectFilter != {}:
            tempProj = {}
            for fields in projectFilter:
                if fields in list(project.keys()):
                    tempProj[fields]=projectFilter[fields]
            if tempProj != {}:
                project = tempProj
        print(list(project.keys()))
        searchFilter={}
        for keys in filter:
            if filter[keys] != 'null':
                if keys == 'doi':
                    searchFilter['DOI']=filter[keys]
                elif keys == 'author':
                    searchFilter['author.authorName']={'$regex':filter[keys]}
                else:
                    searchFilter[keys]={'$regex':filter[keys]}
        #admin abstract, is-refrenced-by-count, ORCID, author.affiliation.name,
        #user publisher, refrence-count, doi, page, title, volume, authorName, container-title, link.url, ISSN, subject
        
        if project == {}:
            data =  collection_name.find(searchFilter).limit(limit).skip(skip)
        else:    
            data =  collection_name.find(searchFilter, project).limit(limit).skip(skip)
        
        
        #data =  collection_name.find(searchFilter).limit(limit).skip(skip)
        dataList = []
        count=0
        for d in data:
            fields = list(d.keys())
            count+=1
            if '_id' in fields:
                d['_id'] = str(d['_id'])
            dataList.append(d)
        query[0]['$match'] = searchFilter
        query[1]['$skip'] = skip
        query[2]['$limit'] = limit
        
        facetData = collection_name.aggregate(query)
        remData={}
        subjectCountList=[]
        journalCountList=[]
        publishYearCountList=[]
        
        for data in facetData:
            # print(data)
            for field in data:
                
                if field == 'Subject':
                    for dicts in data[field]:
                        subject = dicts['_id']
                        pubCount = dicts['Publication Count']
                        citCount = dicts['Citation Count']
                        temp = {'subject' : subject, 'Publication Counts':pubCount, 'Citation Count':citCount}
                        subjectCountList.append(temp)
                        #print(dicts)
                elif field == 'Journal':
                    for dicts in data[field]:
                        journal = dicts['_id'][0]
                        pubCount = dicts['Publication Count']
                        citCount = dicts['Citation Count']
                        temp = {'Journal' : journal, 'Publication Counts':pubCount, 'Citation Count':citCount}
                        journalCountList.append(temp)
                elif field == 'PublishYear':
                    for dicts in data[field]:
                       
                        PublishYear = dicts['_id']
                        pubCount = dicts['Publication Count']
                        citCount = dicts['Citation Count']
                        temp = {'Publish Year' : PublishYear, 'Publication Counts':pubCount, 'Citation Count':citCount}
                        publishYearCountList.append(temp)
                else:
                        # print(data[field][0]['count'])
                        remData[field] = data[field][0]['count']
                    # for items in data[field]:
                    #     print(type(items))
        remData['Subject'] = subjectCountList
        remData['Journal'] = journalCountList
        remData['Publish Year'] = publishYearCountList
        #print(remData['subject'], remData['Publication Count'], remData['Citation Count'])
        if len(dataList) != 0:
            return Response([{'status':'Success','count':str(count)},dataList, remData])
        else:
            return Response([{'count':str(count)},{'status':'Error'}])





class DataByDoi(APIView):
    permission_classes=[IsAdminUser]
    projectFilter=openapi.Parameter('projectFilter', openapi.IN_QUERY, description='Project Filter, Enter {field name:1} to those you want to see', type=openapi.TYPE_OBJECT)
    @swagger_auto_schema(manual_parameters=[projectFilter])
    def get(self, request,doi):
        if request.user.is_superuser:
            userRole = 'superAdmin'
        elif request.user.is_staff:
            userRole = 'admin'
        else:
            userRole = 'user'
        proj = userField.find({'role':userRole},{'project':1,'_id':0})
        project={} 
        for proj in proj:
            project = proj['project']
        projectFilter=json.loads(request.GET.get('projectFilter'))

        if projectFilter != {}:
            tempProj = {}
            for fields in projectFilter:
                if fields in list(project.keys()):
                    tempProj[fields]=projectFilter[fields]
            if tempProj != {}:
                project = tempProj
        if project == {}:
            data =  collection_name.find({'DOI':doi})
        else:    
            data =  collection_name.find({'DOI':doi}, project)
        dataList = []
        count=0
        for d in data:
            count+=1
            d['_id'] = str(d['_id'])
            dataList.append(d)

        if len(dataList) != 0:
            return Response([{'count':str(count)},dataList])
        else:
            return Response([{'count':str(count)},{'statuss':'No match found or incorrect Orcid! Please recheck the Orcid and try'}])
    

    def post(self,request, doi):
        link = "https://api.crossref.org/works/" + doi
        #f = urllib.request.urlopen(link)
        try:
            f = urllib.request.urlopen(link)
            data = (f.read().decode('utf-8'))
            data = json.loads(data)
            try: 
                collection_name.insert_one(data['message'])
                response = {'status':'Data entered!'}
            except errors.DuplicateKeyError:
                response = {'status':'Data regarding this DOI already exists!'}
        except urllib.error.HTTPError as err:
            if err.code == 404:
                response = {'status':'No such DOI found!'}
            else:
                response = {'status':'Error'}
        
        #response = {'status':'ok'}
        return Response(response)




class GetDataByOrcid(APIView):
    permission_classes=[IsAuthenticated]
    page=openapi.Parameter('page', openapi.IN_QUERY, description='Numeric field, works as skip functionality: page 2 count 5 will skip first 5 result and show next five.', type=openapi.TYPE_INTEGER, default=1,required=True)
    count=openapi.Parameter('count', openapi.IN_QUERY, description='Numeric field, works as number of result per execution. count 5 will give 5 result.', type=openapi.TYPE_NUMBER, default=5, required=True)
    filter=openapi.Parameter('filter', openapi.IN_QUERY, description='Search Filter. id: ObjectID, publisher:publisher keywords, doi:DOI, title: title keywords, author:author keywords', type=openapi.TYPE_OBJECT, default={
        'publisher':'null',
        'doi':'null',
        'title':'null',
        'author':'null'
        })
    projectFilter=openapi.Parameter('projectFilter', openapi.IN_QUERY, description='Project Filter, Enter {field name:1} to those you want to see', type=openapi.TYPE_OBJECT)
    @swagger_auto_schema(manual_parameters=[page,count,filter, projectFilter])
    def get(self,request,orcid):
        if request.user.is_superuser:
            userRole = 'superAdmin'
        elif request.user.is_staff:
            userRole = 'admin'
        else:
            userRole = 'user'
        proj = userField.find({'role':userRole},{'project':1,'_id':0})
        project={} 
        for proj in proj:
            project = proj['project']
        print(list(project.keys()))
        limit = int(request.GET.get('count'))
        if (limit < 1):
            return Response({'status':'Error. Count should be minimum 1'})
        skip=(int(request.GET.get('page'))-1)*limit
        if (skip < 0):
            return Response({'status':'Error. Page should be minimum 1'})
        filter = json.loads(request.GET.get('filter'))
        projectFilter=json.loads(request.GET.get('projectFilter'))

        if projectFilter != {}:
            tempProj = {}
            for fields in projectFilter:
                if fields in list(project.keys()):
                    tempProj[fields]=projectFilter[fields]
            if tempProj != {}:
                project = tempProj
        searchFilter={}
        for keys in filter:
            if filter[keys] != 'null':
                if keys == 'doi':
                    searchFilter['DOI']=filter[keys]
                elif keys == 'author':
                    searchFilter['author.authorName']={'$regex':filter[keys]}
                else:
                    searchFilter[keys]={'$regex':filter[keys]}
        searchFilter['author.ORCID'] = orcid            
        # print(orcid, 'here')
        # data =  collection_name.find(searchFilter).limit(limit).skip(skip)

        if project == {}:
            data =  collection_name.find(searchFilter).limit(limit).skip(skip)
        else:    
            data =  collection_name.find(searchFilter, project).limit(limit).skip(skip)

        count=0
        dataList = []
        for d in data:
            count+=1
            d['_id'] = str(d['_id'])
            dataList.append(d)

        query[0]['$match'] = searchFilter
        query[1]['$skip'] = skip
        query[2]['$limit'] = limit
        
        facetData = collection_name.aggregate(query)
        remData={}
        subjectCountList=[]
        journalCountList=[]
        publishYearCountList=[]
        
        for data in facetData:
            # print(data)
            for field in data:
                
                if field == 'Subject':
                    for dicts in data[field]:
                        subject = dicts['_id']
                        pubCount = dicts['Publication Count']
                        citCount = dicts['Citation Count']
                        temp = {'subject' : subject, 'Publication Counts':pubCount, 'Citation Count':citCount}
                        subjectCountList.append(temp)
                        #print(dicts)
                elif field == 'Journal':
                    for dicts in data[field]:
                        journal = dicts['_id'][0]
                        pubCount = dicts['Publication Count']
                        citCount = dicts['Citation Count']
                        temp = {'Journal' : journal, 'Publication Counts':pubCount, 'Citation Count':citCount}
                        journalCountList.append(temp)
                elif field == 'PublishYear':
                    for dicts in data[field]:
                       
                        PublishYear = dicts['_id']
                        pubCount = dicts['Publication Count']
                        citCount = dicts['Citation Count']
                        temp = {'Publish Year' : PublishYear, 'Publication Counts':pubCount, 'Citation Count':citCount}
                        publishYearCountList.append(temp)
                else:
                        # print(data[field][0]['count'])
                        remData[field] = data[field][0]['count']
                    # for items in data[field]:
                    #     print(type(items))
        remData['Subject'] = subjectCountList
        remData['Journal'] = journalCountList
        remData['Publish Year'] = publishYearCountList

        if len(dataList) != 0:
            return Response([{'count':str(count)},dataList, remData])
        else:
            return Response([{'count':str(count)},{'status':'Error'}])




class GetDataByAffiliation(APIView):
    permission_classes=[IsAuthenticated]
    page=openapi.Parameter('page', openapi.IN_QUERY, description='Numeric field, works as skip functionality: page 2 count 5 will skip first 5 result and show next five.', type=openapi.TYPE_INTEGER, default=1,required=True)
    count=openapi.Parameter('count', openapi.IN_QUERY, description='Numeric field, works as number of result per execution. count 5 will give 5 result.', type=openapi.TYPE_NUMBER, default=5, required=True)
    filter=openapi.Parameter('filter', openapi.IN_QUERY, description='Search Filter. id: ObjectID, publisher:publisher keywords, doi:DOI, title: title keywords, author:author keywords', type=openapi.TYPE_OBJECT, default={
        'publisher':'null',
        'doi':'null',
        'title':'null',
        'author':'null'
        })
    filter=openapi.Parameter('filter', openapi.IN_QUERY, description='Search Filter. id: ObjectID, publisher:publisher keywords, doi:DOI, title: title keywords, author:author keywords', type=openapi.TYPE_OBJECT, default={
        'publisher':'null',
        'doi':'null',
        'title':'null',
        'author':'null'
        })
    projectFilter=openapi.Parameter('projectFilter', openapi.IN_QUERY, description='Project Filter, Enter {field name:1} to those you want to see', type=openapi.TYPE_OBJECT)
    @swagger_auto_schema(manual_parameters=[page,count, filter, projectFilter])
    def get(self,request,affil):
        if request.user.is_superuser:
            userRole = 'superAdmin'
        elif request.user.is_staff:
            userRole = 'admin'
        else:
            userRole = 'user'
        proj = userField.find({'role':userRole},{'project':1,'_id':0})
        project={} 
        for proj in proj:
            project = proj['project']
        print(list(project.keys()))

        limit = int(request.GET.get('count'))
        if (limit < 1):
            return Response({'status':'Error. Count should be minimum 1'})
        skip=(int(request.GET.get('page'))-1)*limit
        if (skip < 0):
            return Response({'status':'Error. Page should be minimum 1'})
        affiliation = '/'+affil+'/i'
        filter = json.loads(request.GET.get('filter'))
        projectFilter=json.loads(request.GET.get('projectFilter'))

        if projectFilter != {}:
            tempProj = {}
            for fields in projectFilter:
                if fields in list(project.keys()):
                    tempProj[fields]=projectFilter[fields]
            if tempProj != {}:
                project = tempProj
        searchFilter={}
        for keys in filter:
            if filter[keys] != 'null':
                if keys == 'doi':
                    searchFilter['DOI']=filter[keys]
                elif keys == 'author':
                    searchFilter['author.authorName']={'$regex':filter[keys]}
                else:
                    searchFilter[keys]={'$regex':filter[keys]}
        print(affiliation)
        searchFilter['author.affiliation.name'] = {'$regex':affil}
        # data =  collection_name.find(searchFilter).limit(limit).skip(skip)
        if project == {}:
            data =  collection_name.find(searchFilter).limit(limit).skip(skip)
        else:    
            data =  collection_name.find(searchFilter, project).limit(limit).skip(skip)
        count=0
        dataList = []
        for d in data:
            count+=1
            d['_id'] = str(d['_id'])
            dataList.append(d)
        query[0]['$match'] = searchFilter
        query[1]['$skip'] = skip
        query[2]['$limit'] = limit
        
        facetData = collection_name.aggregate(query)
        remData={}
        subjectCountList=[]
        journalCountList=[]
        publishYearCountList=[]
        
        for data in facetData:
            # print(data)
            for field in data:
                
                if field == 'Subject':
                    for dicts in data[field]:
                        subject = dicts['_id']
                        pubCount = dicts['Publication Count']
                        citCount = dicts['Citation Count']
                        temp = {'subject' : subject, 'Publication Counts':pubCount, 'Citation Count':citCount}
                        subjectCountList.append(temp)
                        #print(dicts)
                elif field == 'Journal':
                    for dicts in data[field]:
                        journal = dicts['_id'][0]
                        pubCount = dicts['Publication Count']
                        citCount = dicts['Citation Count']
                        temp = {'Journal' : journal, 'Publication Counts':pubCount, 'Citation Count':citCount}
                        journalCountList.append(temp)
                elif field == 'PublishYear':
                    for dicts in data[field]:
                       
                        PublishYear = dicts['_id']
                        pubCount = dicts['Publication Count']
                        citCount = dicts['Citation Count']
                        temp = {'Publish Year' : PublishYear, 'Publication Counts':pubCount, 'Citation Count':citCount}
                        publishYearCountList.append(temp)
                else:
                        # print(data[field][0]['count'])
                        remData[field] = data[field][0]['count']
                    # for items in data[field]:
                    #     print(type(items))
        remData['Subject'] = subjectCountList
        remData['Journal'] = journalCountList
        remData['Publish Year'] = publishYearCountList

        if len(dataList) != 0:
            return Response([{'count':str(count)},dataList, remData])
        else:
            return Response([{'count':str(count)},{'status':'Error'}])





#LOGIN LOGOUT API
class LoginView(APIView):
    @swagger_auto_schema(request_body=openapi.Schema(
    type=openapi.TYPE_OBJECT,
    properties={
        'email': openapi.Schema(type=openapi.TYPE_STRING, description="Enter the username here"),
        'password': openapi.Schema(type=openapi.TYPE_STRING, description="Enter password")
        }
    ))
    def post(self,request):
        email=request.data['email']
        password=request.data['password']

        user = authenticate(request,username=email, password=password)
        if user is not None:
            print(user.id) 

            payload = {
                'id':user.id,
                'exp':datetime.datetime.utcnow() + datetime.timedelta(minutes=60),
                'iat': datetime.datetime.utcnow()
            }
            token = jwt.encode(payload, 'secret', algorithm='HS256')
            response = Response() 
            response.set_cookie(key='jwt', value=token, httponly=True)
            response.set_cookie(key='Logged_In', value=True, httponly=True)
            response.data = {
                'jwt':token
            }
            return response
        else:
            raise AuthenticationFailed('Authentication failed, check email or password')
        
class LogoutView(APIView):
    def get(self,request):
        Logged_In = request.COOKIES.get('Logged_In')
        if Logged_In == 'False':
            return Response({'message':'Not Logged In'})
        response = Response()
        response.delete_cookie('jwt')
        response.set_cookie(key='Logged_In', value=False)
        response.data={
                'message':'Successfully logged out',
        } 
        return response







#API WITH MODELS' USE
# class GetData(APIView):
#     def get(self, request):
#         data = outputs.objects.filter(id=ObjectId('6242abba5eb6e305389a13f9'))
#         serializer = outputsSerializer(data)
#         print(data)
#         return Response(serializer.data)

# filter=%7B%0A%20%20%22publisher%22%3A%20%22null%22%2C%0A%20%20%22doi%22%3A%20%22null%22%2C%0A%20%20%22title%22%3A%20%22null%22%2C%0A%20%20%22author%22%3A%20%22null%22%0A%7D
# filter%3D%7B%22publisher%22%3A%20%22null%22%2C%22doi%22%3A%20%22null%22%2C%22title%22%3A%20%22null%22%2C%22author%22%3A%20%22null%22%7D
# http%3A%2F%2F127.0.0.1%3A8000%2FgetData%2F%3Fpage%3D1%26count%3D5%26