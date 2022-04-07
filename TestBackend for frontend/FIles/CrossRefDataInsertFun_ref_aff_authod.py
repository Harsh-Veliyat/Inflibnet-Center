import json, re, os, pymongo
from datetime import date,datetime, timedelta
import pandas as pd
from django.conf import settings
from django.db import connection
import urllib.request
from bson.objectid import ObjectId
cursor = connection.cursor()
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["publication_raw"]

def CheckIssnL(issn):
    url="https://portal.issn.org/resource/ISSN/"+str(issn)+"?format=json"
    json_obj=urllib.request.urlopen(url)
    results=json.load(json_obj)
    issn_l=''
    for r in results["@graph"]:
        if "isPartOf" in r:
            #print("Present, ", end =" ")
            issn_l=r["isPartOf"]
            print("value =", r["isPartOf"])

    return issn_l

def CheckCrossRefDataFun(request,cursor,issn_id,issn) : 
    #issn_id = request.POST.get('issn')
    year = request.POST.get('year')
    toyear = request.POST.get('toyear')
    data = request.POST.get('data_value')
    from_to_to_yr_list = json.loads(request.POST.get('from_to_to_yr_list'))
    created_date = datetime.now()
    ejournal_id_Query = "select id,publisher_name from ejournal_kbart_p2 where online_identifier='"+str(issn_id)+"' or print_identifier='"+str(issn_id)+"' "
    cursor.execute(ejournal_id_Query)
    ejournal_id = cursor.fetchall()

    for eid in ejournal_id:
        update_e_id=eid[0]
        p_nm=eid[1]
        print("update_e_id:::"+str(update_e_id))

    pub_nm_id_Query="select id from publisher_name where publisher_name='"+p_nm+"'"
    cursor.execute(pub_nm_id_Query)
    pub_nm_id = cursor.fetchall()
    for pid in pub_nm_id:
        p_id=pid[0]
    
    multipalyrlst =[]
    if year != toyear:
        idx = [i for i,v in enumerate(from_to_to_yr_list) if v in [str(year),str(toyear)]]
        print(idx) 
        print(from_to_to_yr_list[idx[0]:idx[1]+1])
        multipalyrlst= from_to_to_yr_list[idx[0]:idx[1]+1]
    else:
        multipalyrlst.append(str(year))
    print(multipalyrlst)
    msg_dic={}
    for i in multipalyrlst:
        year= str(i)
        sql_select_Query = "select * from update_log where journal_id='"+str(issn)+"' and year='"+year+"'"
        cursor.execute(sql_select_Query)
        records = cursor.fetchall()
        rcnt=cursor.rowcount
        if rcnt == 0 or rcnt == -1:
            return_tot_cnt = RetrieveCrossrefDataAndInsertInDB("*",0,issn_id,year,update_e_id,p_id,data)
            # print("total no of records return in if part:::"+str(return_tot_cnt[0]))
            # print("counter return in if part:::"+str(return_tot_cnt[1]))
            # print("next_cursor return in if part:::"+str(return_tot_cnt[3]))
            if return_tot_cnt[1] == 0:
                print("Error in 1st data")
            else:
                cursor.execute("INSERT INTO update_log (journal_id,total_record,created_date,last_record_grp_start_from,next_cursor,year,ejournal_id, insert_or_update) \
                        VALUES ('"+issn+"','"+str(return_tot_cnt[0])+"','"+str(created_date)+"' ,'"+str(return_tot_cnt[1])+"','"+str(return_tot_cnt[3])+"', '"+str(year)+"','"+str(update_e_id)+"','"+data+"')")
            msg=str(return_tot_cnt[2])
            msg_dic[year] = msg
            #connection.commit()
        elif rcnt == 1:
            for row in records:
                total=row[2]
                counter=row[5]
                next_cursor=row[6]
                # print("total no of records in elif part:::"+str(row[2]))
                # print("counter in elif part:::"+str(row[5]))
                # print("next_cursor in elif part:::"+str(row[6]))
                #row[2]=total and row[5]=counter and row[6]=next_cursor_data
                if(total == counter or total < counter):
                    print("Data already available in database")
                    msg="Data already available in database"
                    if(data == 'insert'):
                        msg_dic[year] = msg
                        return msg_dic
                    else:  
                        next_cursor = '*'
                        counter = 0
                
            tot_update=RetrieveCrossrefDataAndInsertInDB(next_cursor,counter,issn_id,year,update_e_id,p_id,data)
                # print("TOT come from return::"+str(tot_update))
                # print("total no of records return in elif part inside main:::"+str(tot_update[0]))
                # print("counter return in elif part inside main:::"+str(tot_update[1]))
                # print("next_cursor return in elif part inside main:::"+str(tot_update[3]))
            msg=str(tot_update[2])
            msg_dic[year] = msg
            sql = "UPDATE update_log SET total_record="+str(tot_update[0])+" , last_record_grp_start_from="+str(tot_update[1])+" , next_cursor='"+str(tot_update[3])+"' , insert_or_update='"+str(data)+"' ,updated_date='"+str(created_date)+"'  WHERE journal_id = '"+issn+"' and year='"+year+"' "
            cursor.execute(sql)   
            #connection.commit()            
       
    #connection.close()
    cursor.close()
    return msg_dic
                                                               
def crossref_fixed_field(data_list,columns,solr_content,db_content):
    solr_content['issn']=data_list['ISSN']
    db_content['ISSN']=data_list['ISSN']

    solr_content['url']=data_list['URL']
    db_content['URL']=solr_content['url']

    solr_content['doi_str_mv']=data_list['DOI']
    db_content['DOI']=data_list['DOI']
    columns.append('doi_str_mv')

    if 'alternative-id' in data_list.columns :
        solr_content['alternative-id']= data_list['alternative-id']
        db_content['alternative-id']=data_list['alternative-id']

    if 'issue' in data_list.columns :   
        solr_content['container_issue']=data_list["issue"]
        db_content['issue']=solr_content['container_issue'] 

    if 'page' in data_list.columns :
        solr_content['container_start_page']=data_list['page'] 
        db_content['page']=solr_content['container_start_page']    
                    
    if 'publisher' in data_list.columns :
        solr_content['publisher']=data_list['publisher']
        db_content['publisher']=solr_content['publisher']
        columns.append('publisher')
             
    if 'short-container-title' in data_list.columns :
        solr_content['short_container_title']=data_list['short-container-title'].str.get(0)
        db_content['short-container-title']= data_list['short-container-title']
        columns.append('short_container_title')
        
    if 'source' in data_list.columns : 
        solr_content['source']=data_list['source'] 
        db_content['source']=solr_content['source']
                    
    if 'subject' in data_list.columns :
        solr_content['topic']=data_list['subject']
        db_content['subject']=solr_content['topic']
        columns.append('topic')
                      
    if 'title' in data_list.columns :
        solr_content['title'] = data_list['title'].str.get(0)
        db_content['title'] = data_list['title']

        solr_content['title_short']=data_list['title'].str.get(0)
        regex = re.compile('[^a-zA-Z]')
        title_str=solr_content['title_short'].str.lower()
        solr_content['title_sort']=title_str.str.replace('[^a-zA-Z0-9]', ' ',regex=True)
        columns.append('title')

    if 'subtitle' in data_list.columns :
        solr_content['title_sub']=data_list['subtitle'].str.get(0) 
        db_content['subtitle'] = data_list['subtitle']
        columns.append('title_sub')

    # if 'container-title' in data_list.columns :
    #     solr_content['container_title']=data_list['container-title'].str.get(0)
    #     columns.append('container_title')     
                                    
    if 'volume' in data_list.columns : 
        solr_content['container_volume']=data_list['volume']
        db_content['volume']=solr_content['container_volume']

    if 'member' in data_list.columns : 
        db_content['member'] = data_list['member']
        solr_content['member_str']= data_list['member']
    
    if 'reference-count' in data_list.columns : 
        db_content['reference-count'] = data_list['reference-count']
        solr_content['reference-count']= data_list['reference-count']
        
    if 'is-referenced-by-count' in data_list.columns : 
        db_content['is-referenced-by-count'] = data_list['is-referenced-by-count']


    print("Fixed fields Insertion Completed Successfully")
    return solr_content,db_content,columns
 
def crossref_change_field(data_list,columns,solr_content,p,pd,data_var,update_e_id,db_content):
    import math
    err="blank"
    clean = re.compile('<.*?>')
    pissn, eissn, author, author2, dateSpan, abstract, url_ss,url_tm_ss,url_sc_ss  = [],[],[],[],[],[],[],[],[]
    issn_type, lang, format_static, full_record_str = [],[],[],[]
    jid_i,author_id, affiliation_id, pubNm = [],[],[],[]
    author_original,temp_ref = [],[]
  
    for i in data_var:
        full_record_str.append(json.dumps(i))

    for i in range(0,len(data_list.index)):
        jid_i.append([update_e_id])
        try:            
            if 'update-to' in data_list.columns :
                check_null_update_to=data_list["update-to"][i]
                check_null = False
                if isinstance(check_null_update_to, float):
                    check_null = math.isnan(check_null_update_to)

                if check_null_update_to == 0 or check_null == True:
                    try:
                        if 'type' in data_list.columns :
                            format_static.append([data_list['type'][i]])
                    except:
                        err="Error inside type field"
                else: 
                    update_data=pd.DataFrame(data_list["update-to"][i])
                    format_static.append([update_data['type'][0]])
            else:
                try:
                    if 'type' in data_list.columns :
                        format_static.append([data_list['type'][i]])
                except:
                    err="Error inside type field"
        except:
            err="Error inside update-to field for correction type"

        #For DB Only
        # if 'type' in data_list.columns :
        #     check_dtype=data_list['type'][i]
        #     if(check_dtype == 'journal-article'):
        #         docTypeDescription.append('Articles')
        #         docType.append('ar')
        #     else:
        #         docTypeDescription.append(check_dtype)
        #         docType.append('')   
 
        try:               
            if 'abstract' in data_list.columns :
                check_null_ab = pd.isnull(data_list["abstract"][i])
                if check_null_ab != True:
                    check_null_abstract = data_list["abstract"][i]
                    check_null = False  
                    if isinstance(check_null_abstract, float):
                        check_null = math.isnan(check_null_abstract)
                        
                    if check_null_abstract == 0 or check_null == True:
                        print("Abstract is empty")
                        abstract.append(None)
                    else:   
                        abstract.append(re.sub(clean, '', str(data_list['abstract'][i])))
                else:
                    abstract.append(None)
        except:
            err="Error inside abstract field"

        try:            
            if 'language' in data_list.columns :
                lang.append(p[str(data_list['language'][i])].data)
        except:
            err="Error inside language field"

        #try:          
        if 'author' in data_list.columns :
                created_date= datetime.now() 
                check_null_author=data_list["author"][i]
                check_null = False

                if isinstance(check_null_author, float):
                    check_null = math.isnan(check_null_author)
                
                if check_null_author == 0 or check_null == True:
                    author.append(None)
                    author2.append(None)
                    #author_pipe.append("")
                    author_original.append(None)
                else:  
                        # for i,idx in enumerate(solr_content['id']):
                        #         print("inside i, idx")
                        #         if db_content["author"].iloc[i]!= None:
                        #             print("inside author")
                        #             for itr,j in enumerate(db_content["author"].iloc[i]):
                        #                 if "given" and not "family" in j:
                        #                     print("inside given")
                        #                     j["author_fullnm"]=str(j['given'])
                        #                 elif "family" and not "given" in j:
                        #                     print("inside family")
                        #                     j["author_fullnm"]=str(j['family'])
                        #                 elif "given" and "family" in j:
                        #                     print("inside given and family")
                        #                     j["author_fullnm"]=str(j['given'])+' '+str(j['family'])

                        #                 j["_id"]=str(idx)+'_'+str(itr)
                        #                 print(j)
                        #                 #db.author.insert_one(j)
                        #                 where = { "_id": j["_id"]}
                        #                 setnew={ "$set": j}
                        #                 db.author.update(where,setnew,upsert=True)
                        #                 if "affiliation" in j:
                        #                     for aitr, x in enumerate(j["affiliation"]):
                        #                         x["_id"]=str(j["_id"])+'_'+str(aitr)
                        #                         whereaf = { "_id": x["_id"]}
                        #                         setnewaf={ "$set": x}
                        #                         db.affiliation.update(whereaf,setnewaf,upsert=True)
                   
                    temp_list=data_list["author"].iloc[i]
                    for index in range(len(temp_list)):
                        print(temp_list[index])
                        print("....................")
                        if "given" and not "family" in temp_list[index]:
                            temp_list[index]["author_fullnm"] = str(temp_list[index]["given"])
                        elif "family" and not "given" in temp_list[index]:
                            temp_list[index]["author_fullnm"] = str(temp_list[index]["family"])
                        elif "given" and "family" in temp_list[index]:
                            temp_list[index]["author_fullnm"] = str(temp_list[index]["given"])+" "+str(temp_list[index]["family"])

                        temp_list[index]["author_fullnm"] = str(temp_list[index]["given"])+" "+str(temp_list[index]["family"])
                        
                        temp_dict_del_affiliation = temp_list[index].copy()
                    
                        if "affiliation" in temp_list[index]:
                            del temp_dict_del_affiliation["affiliation"]

                        doi = data_list['DOI'][i]
                        doi_check = db.publication_raw.find({"DOI": doi},{"_id": 1,"author": 1})
                        print(doi_check)
                        if doi_check.count() != 0:
                            for x in doi_check:
                                for j in x.get('author'):
                                    au_ob_id = ObjectId(j['_id'])
                                    print(au_ob_id)
                                    temp_dict_del_affiliation["lastUpdate"]=str(created_date)
                                    where_a = { "_id": au_ob_id}
                                    newvalues_a = { "$set": temp_dict_del_affiliation}
                                    db.author.update(where_a,newvalues_a)
                                    print("author update successfully")
                                    temp_list[index]["_id"]=j['_id']
                                    author_id.append(au_ob_id)
                                    print(author_id)
                                    
                                    if "affiliation" in temp_list[index]:
                                        for aitr, x in enumerate(j["affiliation"]):
                                            af_id = ObjectId(x['_id'])
                                            temp_list[index]["affiliation"][aitr]["lastUpdate"]=str(created_date)
                                            print("inside affiliation")
                                            print(af_id)
                                            where_af = { "_id": af_id}
                                            newvalues_af={ "$set": temp_list[index]["affiliation"][aitr]}
                                            print(where_af)
                                            print(newvalues_af)
                                            db.affiliation.update(where_af,newvalues_af)
                                            print("affliation id update succefully")
                                            del temp_list[index]["affiliation"][aitr]["lastUpdate"]
                                            temp_list[index]["affiliation"][aitr]["_id"] = str(x['_id'])
                                            affiliation_id.append(af_id)
                                            print(affiliation_id)
                                            #db.affiliation.insert(temp_list[index]["affiliation"][aitr])
                                    print("Updated Successfully")

                        else :
                            temp_dict_del_affiliation["lastUpdate"]=str(created_date)
                            db.author.insert(temp_dict_del_affiliation)
                        
                            print("author inserted succefully")
                            aid = db.author.find().sort('_id', pymongo.DESCENDING).limit(1)
                            for dataid in aid:
                                temp_list[index]["_id"] = str(dataid['_id'])
                                author_id.append(dataid['_id'])

                            if "affiliation" in temp_list[index]:
                                for aitr, x in enumerate(temp_list[index]["affiliation"]):
                                    temp_list[index]["affiliation"][aitr]["lastUpdate"] = str(created_date)
                                    db.affiliation.insert(temp_list[index]["affiliation"][aitr])
                                    del temp_list[index]["affiliation"][aitr]["lastUpdate"]
                                    print("affiliation inserted succesfully")
                                    affid = db.affiliation.find().sort('_id', pymongo.DESCENDING).limit(1)
                                    for dataid in affid:
                                        temp_list[index]["affiliation"][aitr]["_id"] = str(dataid['_id'])
                                        affiliation_id.append(dataid['_id'])
                                        #temp_list[index]["affiliation"][aitr]["_id"]=str(temp_list[index]["_id"])+'_'+str(aitr)
                                print("insert affiliation successfully")
                    author_original.append(temp_list)
                    row_data=pd.DataFrame(data_list["author"][i])
                    row_data1=row_data.iloc[[0]]

                    if ('given' in row_data1.columns) and ('family' in row_data1.columns)  :
                        author.append(((row_data1['given'].astype(str)+' '+row_data1['family'].astype(str)).tolist()))   
                        #author_pipe.append("|".join(((row_data1['given'].astype(str)+' '+row_data1['family'].astype(str)).tolist())))
                    elif 'given' not in row_data1.columns and 'family' in row_data1.columns  :
                        author.append(((row_data1['family'].astype(str)).tolist()))   
                        #author_pipe.append("|".join(((row_data1['family'].astype(str)).tolist())))
                    elif 'given' in row_data1.columns and 'family' not in row_data1.columns  :
                        author.append(((row_data1['given'].astype(str)).tolist())) 
                        #author_pipe.append("|".join(((row_data1['given'].astype(str)).tolist())))  
                    
                    if (len(row_data) >=2):
                        row_data2=row_data.iloc[1:]
                        if ('given' in row_data2.columns) and ('family' in row_data2.columns)  :
                            author2.append(((row_data2['given'].astype(str)+' '+row_data2['family'].astype(str)).tolist()))   
                            #author_pipe.append("|".join(((row_data2['given'].astype(str)+' '+row_data2['family'].astype(str)).tolist())))
                        elif 'given' not in row_data2.columns and 'family' in row_data2.columns  :
                            author2.append(((row_data2['family'].astype(str)).tolist()))   
                            #author_pipe.append("|".join(((row_data2['family'].astype(str)).tolist())))
                        elif 'given' in row_data2.columns and 'family' not in row_data2.columns  :
                            author2.append(((row_data2['given'].astype(str)).tolist())) 
                            #author_pipe.append("|".join(((row_data2['given'].astype(str)).tolist())))    
                    else:
                        author2.append(None)
        #except:
        #    err="Error inside author field"

        #try:
        if 'reference' in data_list.columns :
                print("inside ref try block")
                ref_id_list=[]
                temp_ref=data_list["reference"][i]
                #print(temp_ref)
                db_content["reference"]=1
                #print(db_content["reference"])
                if temp_ref != 0:
                    for itr,data in enumerate(temp_ref):
                        #print(itr)
                        data["_id"]="_"+str(itr)
                        ref_id_list.append("_"+str(itr))
                        #print(data["_id"])
                    print(ref_id_list)

                    db_content["reference"].iloc[i] = ref_id_list
        #except:
        #    err="Error inside Ref field"
        
        try:          
            if 'link' in data_list.columns :
                check_null_link_data=data_list["link"][i]
                check_null = False
                
                if isinstance(check_null_link_data, float):
                    check_null = math.isnan(check_null_link_data)

                if check_null_link_data == 0 or check_null == True:
                    url_ss.append(None)
                    url_sc_ss.append(None)
                    url_tm_ss.append(None)
                else:
                    single_link_data = check_null_link_data
                    if len(single_link_data) > 0 :
                        u_ss ,u_tm_ss,u_sc_ss=[],[],[]
                        link_data=pd.DataFrame(single_link_data)
                        for k in range (0,len(link_data)):
                            if link_data["intended-application"][k] == "text-mining":
                                u_tm_ss.append(link_data["URL"][k])
                            elif link_data["intended-application"][k] == "similarity-checking":
                                u_sc_ss.append(link_data["URL"][k])
                            else:
                                u_ss.append(link_data["URL"][k])
                        if len(u_ss) == 0:
                            url_ss.append(None)
                        else:
                            url_ss.append(u_ss)
                        if len(u_sc_ss) == 0:
                            url_sc_ss.append(None)
                        else:
                            url_sc_ss.append(u_sc_ss)
                        if len(u_tm_ss) == 0:
                            url_tm_ss.append(None)
                        else:
                            url_tm_ss.append(u_tm_ss)       
        except:
            err="Error inside link field"
       
        try:            
            if 'issn-type' in data_list.columns :
                check_null_issn_type=data_list["issn-type"][i]
                check_null = False
                if isinstance(check_null_issn_type, float):
                    check_null = math.isnan(check_null_issn_type)

                if check_null_issn_type == 0 or check_null == True:
                    print("Issn-type is empty")
                    issn_type.append(None)
                else: 
                    issn_type.append(check_null_issn_type)     

                    ps= False
                    es= False
                    ref=pd.DataFrame(data_list["issn-type"][i]).fillna(0)
                    for x in range(0,len(ref)):
                        if ref['type'][x] == "print":
                            pissn.append(ref['value'][x])
                            ps= True
                        elif ref['type'][x] =="electronic":
                            eissn.append(ref['value'][x])  
                            es= True
                    if ps == False:
                        pissn.append(None)
                    if es == False:
                        eissn.append(None)
        except:
            err="Error inside issn-type field"

        try:
            if 'published-print' in data_list.columns :
                pubPrintnull = pd.isnull(data_list["published-print"][i])
                if pubPrintnull == True:
                    published_date = 0 
                else:
                    published_date=data_list["published-print"][i]
                    # print("published_date")
                    # print(published_date)
                if published_date == 0 or (len(published_date.keys())==0):
                    if 'published-online' in data_list.columns :
                        pubOnlinenull = pd.isnull(data_list["published-online"][i])
                        if pubOnlinenull == True:
                            published_dateo = 0 
                        else:
                            published_dateo=data_list["published-online"][i]
                            # print("published_dateo")
                            # print(published_dateo)

                        if published_dateo == 0 or (len(published_dateo.keys())==0):
                            dateSpan.append(None)
                        else:
                            dateSpan.append((str(published_dateo['date-parts'][0][0])))                      
                    else:
                            dateSpan.append(None)
                else:
                    dateSpan.append((str(published_date['date-parts'][0][0])))  
                            
            elif 'published-online' in data_list.columns :
                pubOnlinenull = pd.isnull(data_list["published-online"][i])
                if pubOnlinenull == True:
                    published_dateo = 0 
                else:
                    published_dateo=data_list["published-online"][i]

                if published_dateo == 0 or (len(published_dateo.keys())==0):
                    dateSpan.append(None)
                else:
                    dateSpan.append((str(published_dateo['date-parts'][0][0])))
        except:
           err="Error inside published-online and print field"

        try:#online for db
            if 'container-title' in data_list.columns :
                check_null_contitle=data_list['container-title'][i]     
                if check_null_contitle == 0:
                    print("container title is empty")
                    pubNm.append(0)
                else:   
                    pubNm.append('|'.join(map(str, check_null_contitle)))    
        except:
            err="Error inside container-title field"               
                                       
                                 
    if 'author' in data_list.columns :    
        solr_content['author']=pd.Series(author)
        solr_content['author2']=pd.Series(author2)

        #db_content["authors"]=pd.Series(author_pipe)
        db_content["author"] = author_original
        columns.append('author')
        columns.append('author2')
        print("upto author completed") 

    solr_content['publishDate']= dateSpan  
    solr_content['publishDateSort']= dateSpan
    db_content['pubDate']=solr_content['publishDate']  
    print("upto publishDateSort completed")  

    if len(issn_type)>0:
        db_content['issn-type']=issn_type

    if len(pissn)>0:
        solr_content["pissn"]=pissn
        columns.append('pissn')
        print("upto pissn completed") 
        
    if len(eissn) >0:
        solr_content["eissn"]=eissn
        columns.append('eissn')
        print("upto eissn completed") 
        
    if len(abstract)>0:  
        solr_content['description'] = abstract
        db_content['abstract']=abstract
        columns.append('description')
        print("upto description completed") 

    if len(format_static)>0:   
        solr_content['format']= format_static
        print("upto format completed") 

    if 'language' in data_list.columns :   
        solr_content['language'] = lang
        db_content['language'] = lang
        print("upto language completed") 
    
    if len(url_sc_ss)>0:
        solr_content['url_sc_str_mv']= url_sc_ss
        print("upto url_sc_str_mv completed") 

    if len(url_tm_ss)>0:
        solr_content['url_tm_str_mv']= url_tm_ss
        print("upto url_tm_str_mv completed")     

    if len(url_ss)>0:
        solr_content['url_str_mv']= url_ss
        print("upto url_str_mv completed") 

    if len(pubNm)>0:
        solr_content['container_title']= data_list['container-title'].str.get(0)
        #solr_content['container_title_facet']=data_list['container-title'].str.get(0)
        db_content['pubName']=pubNm
        db_content['container-title']= data_list['container-title']
        columns.append('container_title')

    # if len(docTypeDescription)>0:
    #     #solr_content['recordFormat']=data_list['type']
    #     db_content['docTypeDescription']=docTypeDescription
    #     db_content['docType'] = docType  

    #Static fields
    #solr_content['record_format']="crossref-json" 
    solr_content['jid_str']= jid_i
    db_content['db_journal_id']= jid_i
    db_content['fullrecord']= full_record_str
    #solr_content['fullrecord']= full_record_str 

    if err == "blank":
        print("Change Fields Insertion Completed")
    else:
        print(err)
    return solr_content,db_content,columns,err,author_id,affiliation_id,temp_ref

def RetrieveCrossrefDataAndInsertInDB(next_cursor,cnt,issn_id,year,update_e_id,p_id,data):       
    import pysolr
    from habanero import Crossref  
    from jproperties import Properties
    from sqlalchemy import create_engine          
    engine = create_engine('mysql://root:@127.0.0.1/publication_raw?charset=utf8mb4')
    #engine = create_engine('mysql://root:J0@naLA@c#iveDBA$min@127.0.0.1/publication_raw?charset=utf8mb4')
    cr = Crossref()
    cr.journals()
    solr = pysolr.Solr('http://localhost:8080/solr/biblio', timeout=50, always_commit=True)
    start=cnt
    max_cursor=100
    year_val=False
    if (year == "all" or year == "All"):
        year_val = False
    else:
        year_val = True
    print("year_val::"+str(year_val))   
    try:
        while 1:
            try:
                if year_val == False :
                    w1=cr.journals(ids=issn_id, works=True, cursor=next_cursor , cursor_max = max_cursor , limit=100, filter={'type': 'journal-article'})
                else:
                    w1=cr.journals(ids=issn_id, works=True, cursor=next_cursor , cursor_max = max_cursor , limit=100, filter={'from-pub-date' : year,'until-pub-date':year, 'type': 'journal-article'} )
            except:
                print("crossref catch:")
                msg="CrossRef Error"
                tot=0
                return tot,cnt,msg,next_cursor

            if type(w1) == dict:
                tot=w1['message']['total-results']
                data_var= w1["message"]["items"]
            else:
                dic_tot=w1[0]
                tot=dic_tot['message']['total-results']
                data_var= dic_tot["message"]["items"]
                
            print("total:::::::::::::"+str(tot))
            if tot == 0 and year_val == False :
                msg="No data available"
                print(msg)
                return tot,0,msg,next_cursor
            elif tot == 0 and year_val == True :
                msg="No Data available for selected year"
                print(msg)
                return tot,0,msg,next_cursor
            else :
                data_list=pd.DataFrame(data_var).fillna(0)
                data_list_len=len(data_list.index)
                
            if int(tot)>0:
                columns=['issn']

                # Language Properties File open
                p = Properties()
                parent_dir=os.path.join(settings.BASE_DIR)
                with open(""+str(parent_dir)+"/lan.properties", "rb") as f:
                    p.load(f, "utf-8")
                    
                solr_content = pd.DataFrame({'id' : []})
                db_content = pd.DataFrame({'DOI' : []})

                # Fixed solr field Method
                solr_content, db_content, columns = crossref_fixed_field(data_list,columns,solr_content,db_content)
                # Change solr field Method
                solr_content, db_content, columns,err, author_id, affiliation_id, temp_ref = crossref_change_field(data_list,columns,solr_content,p,pd,data_var,update_e_id,db_content)
                errm = err
                
                print("DataFrame db col:::"+str(db_content))
                created_date= datetime.now().strftime('%Y-%m-%d')
                listToStr=[str(i) for i in data_list['DOI'].tolist()]
                db_content['issn_l']= issn_id
                db_content['lastUpdate']= created_date
                db_content['update_pdf']='N'
                db_content['file_name']=''
                db_data=db_content.to_json(orient='records')
                db_insert = json.loads(db_data) 
                check_doi_Query = db.publication_raw.find({"DOI": {"$in": listToStr}},{"_id": 1,"DOI": 1})
                doiqcnt = check_doi_Query.count()
                id_check, doi_check= [],[]
                
                if (check_doi_Query.count()) == 0:
                    db.publication_raw.insert_many(db_insert)
                else:
                    for dataid in check_doi_Query:
                        id_check.append(str(dataid.get('_id')).replace("'",''))
                        doi_check.append(dataid['DOI'].replace("'",''))
                    
                    for i, j in db_content.iterrows(): 
                        j_dict=j.to_dict()
                        j_dict["lastUpdate"]=str(created_date)
                        wherec = { "DOI": j_dict['DOI']}
                        newvalues={ "$set": j_dict}
                        db.publication_raw.update(wherec,newvalues,upsert=True)
                print("MongoDB insertion completed::::::::")  
            
                get_id_solr = db.publication_raw.find({"DOI": {"$in": listToStr}},{"_id": 1,"DOI": 1})
                id_list=[]
                id_list_actual=[]
                for x in get_id_solr:
                    id_list.append(str(x.get('_id')))
                    id_list_actual.append(x.get('_id'))

                solr_content['id'] = id_list 
                print(solr_content['id'])
                solr_content['allfields'] = solr_content[columns].to_dict(orient='records')
                dfdata = pd.DataFrame(solr_content, columns= columns)
                solr_content['allfields'] = dfdata.values.tolist() 
                #print(solr_content['allfields'])
                solr_data=solr_content.to_json(orient='records')
                res = json.loads(solr_data)  
                if errm == "blank":
                    try:
                        solr.add(res)
                        print("Solr insertion completed:::::::")
                        # for insert in id table:::::::::::::::::::::::::::::::::
                        id_table= pd.DataFrame({'ejournal_id' : []})
                        id_table['pub_raw_id'] = id_list
                        id_not_in = (~id_table['pub_raw_id'].isin(id_check))
                        id_table['ejournal_id'] = update_e_id
                        id_table['pub_name_id'] = p_id
                        id_table[id_not_in].to_sql('all_id', con = engine, index=False ,if_exists='append') 
                        if len(temp_ref) > 0:
                            for i,idx in enumerate(solr_content['id']):
                                print("inside i, idx")
                                for itr,j in enumerate(temp_ref):
                                    j["_id"]=str(idx)+'_'+str(itr)
                                    j["lastUpdate"]=str(datetime.now())

                                    where = { "_id": j["_id"]}
                                    setnew={ "$set": j}
                                    db.reference.update(where,setnew,upsert=True)

                        # for i,idx in enumerate(solr_content['id']):
                        #         print("inside i, idx")
                        #         if db_content["author"].iloc[i]!= None:
                        #             print("inside author")
                        #             for itr,j in enumerate(db_content["author"].iloc[i]):
                        #                 if "given" and not "family" in j:
                        #                     print("inside given")
                        #                     j["author_fullnm"]=str(j['given'])
                        #                 elif "family" and not "given" in j:
                        #                     print("inside family")
                        #                     j["author_fullnm"]=str(j['family'])
                        #                 elif "given" and "family" in j:
                        #                     print("inside given and family")
                        #                     j["author_fullnm"]=str(j['given'])+' '+str(j['family'])

                        #                 j["_id"]=str(idx)+'_'+str(itr)
                        #                 print(j)
                        #                 #db.author.insert_one(j)
                        #                 where = { "_id": j["_id"]}
                        #                 setnew={ "$set": j}
                        #                 db.author.update(where,setnew,upsert=True)
                        #                 if "affiliation" in j:
                        #                     for aitr, x in enumerate(j["affiliation"]):
                        #                         x["_id"]=str(j["_id"])+'_'+str(aitr)
                        #                         whereaf = { "_id": x["_id"]}
                        #                         setnewaf={ "$set": x}
                        #                         db.affiliation.update(whereaf,setnewaf,upsert=True)

                
                        # end:::::::::::::::::::::::::::::
                        if type(w1) == dict:
                            next_cursor = w1['message']['next-cursor']
                        else:
                            next_cursor = w1[0]['message']['next-cursor']
                        print("next_cursor::"+str(next_cursor))
                        start=start+100
                        print("start::"+str(start))
                        cnt=cnt+data_list_len
                        print("cnt::"+str(cnt))
                        print("tot::"+str(tot))  
                        fmsg="Data "+str(data)+" Successfully"
                    except Exception as maine:
                        query_string=db.publication_raw.remove({'_id':{"$in": id_list_actual}})
                        query_string=db.affiliation.remove({'_id':{"$in": affiliation_id}})
                        query_string=db.author.remove({'_id':{"$in": author_id}})
                        
                        print("Record deleted successfully:::"+str(maine))
                        fmsg="Solr Error"
                        break
                else:
                    query_string = db.publication_raw.remove({'_id':{"$in": id_list_actual}})
                    query_string=db.affiliation.remove({'_id':{"$in": affiliation_id}})
                    query_string=db.author.remove({'_id':{"$in": author_id}})

                    print("Record deleted successfully"+str(maine))
                    fmsg="Error"
                    break
            else:
                print("no publication report found")
                msg="No publication records found"                
            if int(tot)-start>0:
                max_cursor = min(max_cursor, int(tot)-start)
                print("max_cursor::"+str(max_cursor))
            else:
                break       
        msg=str(fmsg)
        return tot,cnt,msg,next_cursor
    except Exception as maine:
        print("Error at a time of Execution in main try block:::::::"+str(maine))
        msg="Error at a time of Execution"
        return tot,cnt,msg,next_cursor

#Not Requriedd below to methods:::::::::::::::::::::::::::::::::::::   
def solr_data_delete():
    import pysolr
    solr = pysolr.Solr('http://localhost:8080/solr/biblio', timeout=10, always_commit=True)
    solr.delete(q='*:*')
    print("solr all data deleted successfully::::::::::::::") 


    
    
                       
           
