#from habanero import Crossref
#from operator import ilshift
#from urllib import request, response
from types import NoneType
from bson import ObjectId
from pymongo import MongoClient, errors
import json
from .models import outputs
from .serializers import outputsSerializer


from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.parsers import JSONParser
from django.http import JsonResponse
from bson import ObjectId

from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema

import urllib
client = MongoClient('localhost',
                     username='hveliyat',
                     password='qwertyuiop',
                     authSource='admin',
                     authMechanism='SCRAM-SHA-1')
db = client['crossrefTest']#user-manage
# collection_publisher = db["publisher"] #  publisher
# collection_author = db["author"] # author member
# collection_journal = db['journal'] #doi, refrence-count, short-container-title, title , ISSN, ISSN type, subject
collection_name = db['outputs']
# collection_member = db['member']

# data = collection_name.find_one({'_id':ObjectId('6229db91017d7991ce8bd5f7')})
#         #data = JSONParser().parse(data)
# print(type(data))
# data = jsonify()
#print(collection_name.find({}).limit(10))



# [
#     {
#         '$facet': {
#             'subject': [
#                 {
#                     '$unwind': '$subject'
#                 }, {
#                     '$sortByCount': '$subject'
#                 }
#             ], 
#             'publication count': [
#                 {
#                     '$count': 'count'
#                 }
#             ], 
#             'citation count': [
#                 {
#                     '$group': {
#                         '_id': '$null', 
#                         'sum': {
#                             '$sum': '$is-referenced-by-count'
#                         }
#                     }
#                 }
#             ]
#         }
#     }
# ]

##FACET QUERY MONGODB
# [
#     {
#         '$match': {
#             'short-container-title': 'Banks and Bank Systems'
#         }
#     }, {
#         '$facet': {
#             'subject': [
#                 {
#                     '$unwind': '$subject'
#                 }, {
#                     '$sortByCount': '$subject'
#                 }
#             ], 
#             'publication count': [
#                 {
#                     '$count': 'count'
#                 }
#             ], 
#             'citation count': [
#                 {
#                     '$group': {
#                         '_id': '$null', 
#                         'sum': {
#                             '$sum': '$is-referenced-by-count'
#                         }
#                     }
#                 }
#             ]
#         }
#     }
# ]



#API START


query = [
    {
        '$limit': 5
    }, {
        '$facet': {
            'subject': [
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
            'Publication Count': [
                {
                    '$count': 'count'
                }
            ], 
            'Citation Count': [
                {
                    '$group': {
                        '_id': '$null', 
                        'count': {
                            '$sum': '$is-referenced-by-count'
                        }
                    }
                },
                {'$project':{'_id':0}}
            ]
        }
    }
]


class GetDataForReact(APIView):
    def post(self,request, limit):
        data =  collection_name.find({},{"title":1 ,"DOI":1, "author":1}).limit(limit)
        count = 0
        Sr=1
        dataList = []
        for d in data:
            count+=1
            d['_id'] = str(d['_id'])
            authorCount=0
            if d['author'] != None:
                for authors in d['author']:
                    affCount=0
                    for aff in authors['affiliation']:
                        temp={}
                        temp['Sr']=Sr
                        temp['authorId']=authorCount
                        temp['affiliationId']=affCount
                        temp['_id']=d['_id']
                        temp['title']=d['title'][0]
                        temp['DOI']=d['DOI']
                        if 'given' in list(authors.keys()):
                            temp['given']=authors['given']
                            temp['authorName']=authors['authorName']
                        else:
                            temp['given']= "Not Available"
                            temp['authorName']=authors['family']
                        temp['family']=authors['family']
                        if 'ORCID' in list(authors.keys()):
                            temp['ORCID']=authors['ORCID']
                        else:
                            temp['ORCID']='not available'
                        if 'name' in list(aff.keys()):
                            temp['affiliation']=aff['name']
                        else:
                            temp['affiliation']='Not Available'
                        dataList.append(temp)
                        affCount+=1
                        Sr+=1
                    authorCount+=1

        print(count)
        if len(dataList) != 0:
            return Response(dataList)
        else:
            return Response({'statuss':'Error'})

class GetDataFromReact(APIView):
    def post(self,request,oid):#Year's Death   Doctor, Lecturer, Faculty of Economics and Business, Management science Department, University Halu Oleo Kendari
        if request.data['field'] == 'title':
            lists=[]
            lists.append(request.data['value'])
            request.data['value'] = lists
            field = request.data['field']
            
        elif request.data['field'] == 'name':
            field = 'author.'+str(request.data['authorId'])+'.affiliation.'+str(request.data['affiliationId'])+'.'+request.data['field']
            # collection_name.update({'_id':ObjectId(oid)},{'$set':{field:request.data['value']}})
        else:
            field = 'author.'+str(request.data['authorId'])+'.'+request.data['field']
            field2 = 'author.'+str(request.data['authorId'])+'.authorName'
            if request.data['field'] == 'given':
                authorName = request.data['value'] + ' ' + request.data['otherName']
            else:
                authorName = request.data['otherName'] + ' ' + request.data['value']
                
            collection_name.update({'_id':ObjectId(oid)},{'$set':{field:request.data['value'], field2:authorName}})
            return Response({'status':'ok'})
        
        collection_name.update({'_id':ObjectId(oid)},{'$set':{field:request.data['value']}})
        
        return Response({'status':'ok'})

class GetData(APIView):
    def get(self, request):
        data =  collection_name.find({}).limit(5)
        dataList = []
        count=0
        for d in data:
            count+=1
            d['_id'] = str(d['_id'])
            dataList.append(d)
            dataList.append({})
        print(query[0]['$limit'])
        facetData = collection_name.aggregate(query)
        remData={}
        subjectCountList=[]
        # for data in facetData:
        #     for eachData in data:
        #         if eachData == 'subject':
        #             print(eachData , ':')
        #             for subjects in data[eachData]:
        #                 #subjectCountList.append({subjects['_id']:subjects['count']})
        #                 print(subjects)
        #         else:
        #             pubAndCitCount = list(data[eachData][0].values())
        #             remData[eachData]=pubAndCitCount[len(pubAndCitCount)-1]
        # remData['subject']=subjectCountList
        for data in facetData:
            for field in data:
                #print(field," : ",data[field])
                if field == 'subject':
                    for dicts in data[field]:
                        subject = dicts['_id']
                        pubCount = dicts['Publication Count']
                        citCount = dicts['Citation Count']
                        temp = {'subject' : subject, 'Publication Counts':pubCount, 'Citation Count':citCount}
                        subjectCountList.append(temp)
                        #print(dicts)
                else:
                        #print(data[field][0]['count'])
                        remData[field] = data[field][0]['count']
                    # for items in data[field]:
                    #     print(type(items))
        remData['subject'] = subjectCountList
        #print(remData['subject'], remData['Publication Count'], remData['Citation Count'])
        if len(dataList) != 0:
            return Response([{'count':str(count)},dataList, remData])
        else:
            return Response([{'count':str(count)},{'statuss':'Error'}])





class DataByDoi(APIView):
    def get(self, request,doi):
        data =  collection_name.find({'DOI':doi})
        dataList = []
        count=0
        for d in data:
            count+=1
            d['_id'] = str(d['_id'])
            dataList.append(d)
            dataList.append({})

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
    def get(self,request,orcid):
        print(orcid, 'here')
        data =  collection_name.find({'author.ORCID':orcid})
        count=0
        dataList = []
        for d in data:
            count+=1
            d['_id'] = str(d['_id'])
            dataList.append(d)
            dataList.append({})

        if len(dataList) != 0:
            return Response([{'count':str(count)},dataList])
        else:
            return Response([{'count':str(count)},{'statuss':'No match found or incorrect Orcid! Please recheck the Orcid and try'}])



#API END



# class GetDataByAffiliation(APIView):
#     def get(self,request,affilliation):
        
#         data =  collection_name.find({"author.affiliation":affilliation})
#         count=0
#         dataList = []
#         for d in data:
#             count+=1
#             d['_id'] = str(d['_id'])
#             dataList.append(d)
#             dataList.append({})

#         if len(dataList) != 0:
#             return Response([{'count':str(count)},dataList])
#         else:
#             return Response([{'count':str(count)},{'statuss':'No match found or incorrect Orcid! Please recheck the Orcid and try'}])


# class GetDataByROR(APIView):
#     def get(self,request,ror):
        
#         data =  collection_name.find({'author.ORCID':ror})
#         dataList = []
#         for d in data:
#             d['_id'] = str(d['_id'])
#             dataList.append(d)
#             dataList.append({})

#         if len(dataList) != 0:
#             return Response(dataList)
#         else:
#             return Response({'statuss':'No match found or incorrect ROR! Please recheck the ROR and try'})





###ONE ENDPOINT EXPERIMENT
# class GetData(APIView):
#     def get(self, request,doi):
#         print(request.path)
#         data =  collection_name.find({'DOI':doi})
#         print('here')
#         dataList = []
#         for d in data:
#             d['_id'] = str(d['_id'])
#             dataList.append(d)
#             dataList.append({})

#         if len(dataList) != 0:
#             return Response(dataList)
#         else:
#             return Response({'status':'No match found or incorrect DOI! Please recheck the DOI and try'})
    
#     # def get(self, request, orcid):
    #     data =  collection_name.find({'ORCID':orcid})
    #     dataList = []
    #     count = 0
    #     for d in data:
    #         count+=1
    #         d['_id'] = str(d['_id'])
    #         dataList.append(d)
    #         dataList.append({})

    #     if len(dataList) != 0:
    #         return Response(count,dataList)
    #     else:
    #         return Response(count,{'status':'No match found or incorrect ORCID! Please recheck the ORCID and try'})







    # AFFIL = openapi.Parameter('affil', openapi.IN_QUERY, description='Enter the DOI to search result', type=openapi.TYPE_STRING)
    # @swagger_auto_schema(manual_parameters=[DOI],operation_description='Get data from MongoDB by DOI')
    # def get(self, request):
        
    #     doi = request.GET.get('doi')
    #     data =  collection_name.find({'DOI':doi})
    #     dataList = []
    #     for d in data:
    #         d['_id'] = str(d['_id'])
    #         dataList.append(d)
    #         dataList.append({})

    #     if len(dataList) != 0:
    #         return Response(dataList)
    #     else:
    #         return Response({'status':'No match found or incorrect DOI! Please recheck the DOI and try'})
    


    # ROR = openapi.Parameter('ror', openapi.IN_QUERY, description='Enter the DOI to search result', type=openapi.TYPE_STRING)
    # @swagger_auto_schema(manual_parameters=[DOI],operation_description='Get data from MongoDB by DOI')
    # def get(self, request):
        
    #     doi = request.GET.get('doi')
    #     data =  collection_name.find({'DOI':doi})
    #     dataList = []
    #     for d in data:
    #         d['_id'] = str(d['_id'])
    #         dataList.append(d)
    #         dataList.append({})

    #     if len(dataList) != 0:
    #         return Response(dataList)
    #     else:
    #         return Response({'status':'No match found or incorrect DOI! Please recheck the DOI and try'})
        
    # MemberID = openapi.Parameter('memid', openapi.IN_QUERY, description='Enter the DOI to search result', type=openapi.TYPE_STRING)
    # @swagger_auto_schema(manual_parameters=[DOI],operation_description='Get data from MongoDB by DOI')
    # def get(self, request):
        
    #     doi = request.GET.get('doi')
    #     data =  collection_name.find({'DOI':doi})
    #     dataList = []
    #     for d in data:
    #         d['_id'] = str(d['_id'])
    #         dataList.append(d)
    #         dataList.append({})

    #     if len(dataList) != 0:
    #         return Response(dataList)
    #     else:
    #         return Response({'status':'No match found or incorrect DOI! Please recheck the DOI and try'})
    
    # AISHE_CODE = openapi.Parameter('aishecode', openapi.IN_QUERY, description='Enter the DOI to search result', type=openapi.TYPE_STRING)
    # @swagger_auto_schema(manual_parameters=[DOI],operation_description='Get data from MongoDB by DOI')
    # def get(self, request):
        
    #     doi = request.GET.get('doi')
    #     data =  collection_name.find({'DOI':doi})
    #     dataList = []
    #     for d in data:
    #         d['_id'] = str(d['_id'])
    #         dataList.append(d)
    #         dataList.append({})

    #     if len(dataList) != 0:
    #         return Response(dataList)
    #     else:
    #         return Response({'status':'No match found or incorrect DOI! Please recheck the DOI and try'})







#INITIATE CROSSREF CONNECTION HERE
# cr = Crossref()


# print(datetime.now())



# data = cr.works(filter={'has-affiliation':True, 'type':'journal-article'}, cursor='*')
# insert = collection_name.insert_one(data[3]['message'])
# id = insert.inserted_id
# # datas = collection_name.find({'_id':ObjectId('6225dfa9266dfd93962750a1')})
# # index=datas[0]['total-results']
# datas = collection_name.find({'_id':ObjectId(id)})
# next_cursor = datas[0]['next-cursor']



# while data[3]['message']['next-cursor'] == next_cursor:
#     data = cr.works(filter={'has-affiliation':True, 'type':'journal-article'}, cursor=next_cursor)
#     collection_name.update_one({'_id':ObjectId(id)},{'$push':{'items':{'$each':data[3]['message']['items']}}})
#     print(datetime.now())


# datas = collection_name.find({'_id':ObjectId('6228357aa95a20dea3bc66e4')})


# skip = round(len(datas[0]['items'])/20) - 1
# print(skip)


# data = cr.works(filter={'has-affiliation':True, 'type':'journal-article'},cursor='*')
# print(data)


#DATA FETCH
# link = "https://api.crossref.org/works?filter=has-affiliation:true,type:journal-article&cursor=*&rows=1000"
# #f = urllib.urlopen(link)
# f = urllib.request.urlopen(link)
# data = (f.read().decode('utf-8'))
# data = json.loads(data)
# print(data['message']['items-per-page'])

# next_cursor= data['message']['next-cursor']
# print(type(data['message']['items']))
# for i in range(data['message']['items-per-page']):
#     collection_name.insert_one(data['message']['items'][i])
# #id = '622983c3a8fb163555516620'



# # WORKING DATA TRANSFER.
# link = "https://api.crossref.org/works?filter=has-affiliation:true,type:journal-article&cursor="+next_cursor+"&rows=1000"
# while data['message']['next-cursor'] == next_cursor:
#     f = urllib.request.urlopen(link)
#     data = (f.read().decode('utf-8'))
#     data = json.loads(data)
#     #collection_name.update_one({'_id':ObjectId(id)},{'$push':{'items':{'$each':data['message']['items']}}})
#     for i in range(data['message']['items-per-page']):
#        collection_name.insert_one(data['message']['items'][i])

# print("Done!")


#### Members list obtained from 10400 data inserted already to members.txt
# data = collection_name.find({})
# list = []
# count=0
# for i in data:
#     #print(type(i['member']))
#     if 'member' in i.keys():
#         list.append(int(i['member']))
#         count+=1
# print(list, file=open("members.txt","w",encoding='utf-8'))


##  DOI 
# data = collection_name.find({})
# list = []
# count=0
# for i in data:
#     if 'DOI' in i.keys():
# #        if i['DOI'] not in DOI_list:
#             list.append(i['DOI'])
#             count+=1
# print(list, file=open("DOI.txt","w",encoding='utf-8'))
# print(count)


# ##CHECK FOR DUPLICATE DOI
# data = open("DOI.txt", 'r')
# data = data.read()
# DOI_list = data.strip('][').split(', ')
# setd = set(DOI_list)

# print(len(setd) != len(DOI_list))



# ## CHECKING DOI LIST
# data = open("DOI.txt", 'r')
# data = data.read()
# DOI_list = data.strip('][').split(', ')

# link = "https://api.crossref.org/works?filter=has-affiliation:true,type:journal-article&cursor=*&rows=1000"
# f = urllib.request.urlopen(link)    
# data = (f.read().decode('utf-8'))
# data = json.loads(data)
# next_cursor= data['message']['next-cursor']
# print('here')

# for i in range(data['message']['items-per-page']):
#     if data['message']['items'][i]['DOI'] not in DOI_list:
#         print('first for')
#         collection_name.insert_one(data['message']['items'][i])
#         list=data['message']['items'][i]['DOI']
#         print(list, file=open("DOI.txt","a",encoding='utf-8'))
#     else:
#         print("in first for but not if", data['message']['items'][i]['DOI'])
# print(datetime.now())


# link = "https://api.crossref.org/works?filter=has-affiliation:true,type:journal-article&cursor="+next_cursor+"&rows=1000"
# while data['message']['next-cursor'] == next_cursor:
#     f = urllib.request.urlopen(link)
#     data = (f.read().decode('utf-8'))
#     data = json.loads(data)
#     for i in range(data['message']['items-per-page']):
#         if data['message']['items'][i]['DOI'] not in DOI_list:
#             print('second for')
#             collection_name.insert_one(data['message']['items'][i])
#             list=data['message']['items'][i]['DOI']
#             print(list, file=open("DOI.txt","a",encoding='utf-8'))
 
#         else:
#             print("in second for but not if", data['message']['items'][i]['DOI'])
#     print(datetime.now())

# print("Done!")






# for line in f:
# 	data = line.decode("utf-8")
# 	print(data, file=open("output.txt", 'w', encoding='utf-8'))
    
# print(type(dict(data)))


#print(data['status'], type(data))
#file=open("output.txt", 'w', encoding='utf-8')





# DETAILS WE WANT:
#    #https://api.crossref.org/v1/works/10.1002/jnr.490170206

#Query:
#    #BASED ON QUERY:::: https://api.crossref.org/works?query.affiliation=gandhinagar&filter=has-affiliation:true,type:journal-article,from-pub-date:2022,limit:20
#BASED ON DOI:::::::::::::https://api.crossref.org/works/10.21511/bbs.13(3).2018.13


# db.outputs.find('author.authorName':{'$exists':false}).forEach(function (doc){
#   for(var i in doc.author){
#     doc.author[i].authorName = doc.author[i].given + ' ' + doc.author[i].family
#   }
#   print(doc.author)
#   db.outputs.update({'_id':ObjectId(doc._id)},{'$set':{'author':doc.author}})
# })