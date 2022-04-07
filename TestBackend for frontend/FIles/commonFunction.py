import re,json,pymongo,pysolr,os, mysql.connector,sys,math
#from datetime import datetime, date
from habanero import Crossref 
#from jproperties import Properties
import urllib.request
# import pandas as pd
import dbConnection

# MongoDb Connection
client = pymongo.MongoClient(dbConnection.mongoClient)
db = client[dbConnection.mongoDB]

# # Dataframe insert in mysql
# from sqlalchemy import create_engine          
# engine = create_engine(dbConnection.mysqlEngine)

# # MySQL Connection
# myconn = mysql.connector.connect(host = dbConnection.mysqlHost, user = dbConnection.mysqlUser, passwd = dbConnection.mysqlPwd, database = dbConnection.mysqlDatabase)  
# cursor = myconn.cursor()

# # Solr Connection
# solr = pysolr.Solr(dbConnection.solrConnection, timeout=500, always_commit=True)

# Crossref
cr = Crossref(mailto = "eshodhsindhu@inflibnet.ac.in")
cr.journals()

# Language Properties File open
# p = Properties()

#with open(""+str(parent_dir)+"/lan.properties", "rb") as f:
with open(dbConnection.lanFilePath, "rb") as f:
    p.load(f, "utf-8")

#created_date = datetime.now()
created_date = datetime.now().strftime('%Y-%m-%d')
dtime = str( datetime.utcnow() )

#Check ISSN is valid or not
def validateIssn(issn):
    #reg = r'^\d{4}\-\d{4}$'
    reg = r'^\d{4}\-\d{3}[0-9|x|X]{1}$'
    if re.match(reg, issn):
        return True
    else:
        return False

#CheckIssnlFromIssnSite
def checkIssnL(issn):
    url="https://portal.issn.org/resource/ISSN/"+str(issn)+"?format=json"
    #print(url)
    json_obj=urllib.request.urlopen(url)
    results=json.load(json_obj)
    issn_l=''
    for r in results["@graph"]:
        if "isPartOf" in r:
            issn_l=r["isPartOf"]
            issn_l= issn_l[-9:]
            #print("from portal issn site value =", r["isPartOf"])
    return issn_l

def fetchJournalID(issn_id):
    if type(issn_id) != list:
        flag=True
        ejournal_id_Query = "select id,issnl from survey_ejournals where online_identifier='"+str(issn_id)+"' or print_identifier='"+str(issn_id)+"' ORDER BY id asc limit 1"
    else:
        flag=False
        issn_id=str(issn_id).replace("[",'').replace(']','')
        issn_id=issn_id.replace("'","").replace(", ","','")
        ejournal_id_Query = "select id,issnl,print_identifier,online_identifier from survey_ejournals where online_identifier in('"+str(issn_id)+"') or print_identifier in('"+str(issn_id)+"') ORDER BY id asc limit 1"
    cursor.execute(ejournal_id_Query)
    ejournal_id = cursor.fetchall()
    if len(ejournal_id) > 0:
        for eid in ejournal_id:
            ejou_id = eid[0] 
            issnl = eid[1] 
            if flag == True:
                if issnl == None or issnl == '' or issnl == 'Null': 
                    print("if")  
                else:    
                    issn_id = issnl
            else:
                if issnl == None or issnl == '' or issnl == 'Null': 
                    if eid[2] == None or eid[2] == '' or eid[2] == 'Null':
                        issn_id = eid[3]
                    else:
                        issn_id = eid[2]

            issnl = checkIssnL(issn_id)          
    else:
        print("No Data availble for "+str(issn_id)+" in survey_ejournals table")
        sys.exit()

    return ejou_id,issnl

def createYearListFromToYear(frmyear,toyear):
    frmyear=int(frmyear)
    toyear=int(toyear)
    todays_date = date.today()
    multipalyrlst =[]
    if frmyear != toyear:
        if toyear == 9999 :
            for i in range(frmyear,todays_date.year+1):
                multipalyrlst.append(frmyear)
                frmyear = frmyear+1
        else:
            for i in range(frmyear,toyear+1):
                multipalyrlst.append(frmyear)
                frmyear = frmyear + 1
    else:
        multipalyrlst.append(str(frmyear))
    return multipalyrlst

def fetchSolrJournalIdCount(jid):
    result = solr.search('*:*', **{
    'rows': 0,
    'facet': 'true',
    'facet.query': {'jid_str:"'+str(jid)+'"'}
})   
    facetCount=result.facets["facet_queries"]['jid_str:"'+str(jid)+'"']
    #print(facetCount)
    return facetCount

def fetchMongoDBJournalIdCount(jid):
    mDBjidcount=db.publication_raw.find({'journal_id': jid}).count()
    #print(mDBjidcount)
    return mDBjidcount

def addDataInUpdatedStatusSQL(jid,status,method,noOfRecords):
    # journalid (-survey_journals)
    # last_updated_date
    # last_updated_status (error/successful)
    # last_update_method (eg. cronjob/CLI/ GUI, etc)
    # records_fetched/updated(no of records)
    UpdateDate = created_date
    query="INSERT INTO updated_status(journalid,last_updated_date,last_updated_status,last_update_method,records_fetched) VALUES ('"+str(jid)+"','"+str(UpdateDate)+"','"+str(status)+"','"+str(method)+"','"+str(noOfRecords)+"')"
    #print(query) 
    cursor.execute(query)                 
    myconn.commit()
    #print("Data inserted in UpdatedStatus table in mysql")

def updateDataInUpdatedFrequencySQL(jid):
    # last_checked_date (date-checked )
    # No_documents_solr (as on last_checked_date)
    # No_documents_db (as on last_checked_date)
    lastCheckedDate = created_date
    totNoDocSolr = fetchSolrJournalIdCount(jid)
    totNoDocDB = fetchMongoDBJournalIdCount(jid)
    query = "UPDATE update_frequency SET last_checked_date='"+str(lastCheckedDate)+"' , No_documents_solr='"+str(totNoDocSolr)+"' , No_documents_db='"+str(totNoDocDB)+"' WHERE journalid = '"+str(jid)+"' "
    cursor.execute(query)                 
    myconn.commit()
    #print("Data updated in update_frequency table in mysql")

def ErrorInAnyFieldAddDataInessCheckDataSQL(uniqueId,source,errorMsg):
    cursor.execute("INSERT INTO ess_checkdata (uniqueId,source,note,createdDate,flag) \
                                VALUES ('"+str(uniqueId)+"','"+str(source)+"','"+str(errorMsg)+"' ,'"+str(created_date)+"',0)")                       
    myconn.commit()

def crossrefFixedField(data_list,columns,solr_content,db_content,debug):
    solr_content['issn'] = data_list['ISSN']
    db_content['ISSN'] = data_list['ISSN']
    solr_content['url'] = data_list['URL']
    db_content['URL'] = data_list['URL']
    
    if debug == 'T' or debug == 't':
        print(data_list['DOI'].to_string())

    solr_content['doi_str_mv']=data_list['DOI']
    db_content['DOI'] = data_list['DOI']
    columns.append('doi_str_mv')

    if 'alternative-id' in data_list.columns :
        solr_content['alternative-id']= data_list['alternative-id']
        db_content['alternative-id'] = data_list['alternative-id']

    if 'issue' in data_list.columns :   
        solr_content['container_issue']=data_list["issue"]
        db_content['issue'] = data_list["issue"]

    if 'page' in data_list.columns :
        solr_content['container_start_page']=data_list['page'] 
        db_content['page'] = data_list['page']     

    if 'publisher' in data_list.columns :
        solr_content['publisher']=data_list['publisher']
        columns.append('publisher')
        db_content['publisher'] = data_list['publisher']
             
    if 'short-container-title' in data_list.columns :
        solr_content['short_container_title']=data_list['short-container-title'].str.get(0)
        columns.append('short_container_title')
        db_content['short-container-title'] = data_list['short-container-title']

    if 'source' in data_list.columns : 
        solr_content['source']=data_list['source'] 
        db_content['source'] = data_list['source']
                    
    if 'subject' in data_list.columns :
        solr_content['topic']=data_list['subject']
        columns.append('topic')
        db_content['subject'] = data_list['subject']
                      
    if 'title' in data_list.columns :
        solr_content['title'] = data_list['title'].str.get(0)
        solr_content['title_short']=data_list['title'].str.get(0)
        regex = re.compile('[^a-zA-Z]')
        title_str=solr_content['title_short'].str.lower()
        #solr_content['title_sort']=title_str.str.replace('[^a-zA-Z0-9]', ' ',regex=True)
        solr_content['title_sort']=title_str.str.replace('[^a-zA-Z0-9\s+]', '', regex=True)
        columns.append('title')
        db_content['title'] = data_list['title']

        # try to create id using title
        title_id_str= data_list['title'].str.get(0).replace('\s+(a|an|the|A|AN|THE|An|The)(\s+)', '', regex=True)
        title_id_str= title_id_str.str.replace('(a|an|the|A|AN|THE|An|The)(\s+)', '', regex=True)
        db_content['title_id_str']= title_id_str.str.replace('[\':;.,"-]','', regex=True).replace(' ','',regex=True)
        solr_content['title_id_str'] = db_content['title_id_str']
    
    if 'original-title' in data_list.columns :
        solr_content['title_alt'] = data_list['original-title'].str.get(0) 
        db_content['original-title'] = data_list['original-title'].str.get(0) 
        columns.append('title_alt')
        
    if 'subtitle' in data_list.columns :
        solr_content['title_sub']=data_list['subtitle'].str.get(0) 
        columns.append('title_sub')    
        db_content['subtitle'] = data_list['subtitle'] 
                                    
    if 'volume' in data_list.columns : 
        solr_content['container_volume']=data_list['volume']
        db_content['volume'] = data_list['volume']

    if 'member' in data_list.columns : 
        solr_content['member_str']= data_list['member']
        db_content['member'] = data_list['member']
    
    if 'reference-count' in data_list.columns : 
        solr_content['reference-count']= data_list['reference-count'] 
        db_content['reference-count'] = data_list['reference-count']    
        
    if 'is-referenced-by-count' in data_list.columns : 
        db_content['is-referenced-by-count'] = data_list['is-referenced-by-count']

    if 'container-title' in data_list.columns :
        solr_content['container_title']=data_list['container-title'].str.get(0)
        solr_content['container_title_facet']=data_list['container-title'].str.get(0)
        db_content['container-title'] = data_list['container-title']
        columns.append('container_title')    

        # try to create id using title
        con_title_str= data_list['container-title'].str.get(0).replace('\s+(a|an|the|A|AN|THE|An|The)(\s+)', '', regex=True)
        con_title_str= con_title_str.str.replace('(a|an|the|A|AN|THE|An|The)(\s+)', '', regex=True)
        db_content['jou_title_id_str']= con_title_str.str.replace('[\':;.,"-]', '', regex=True).replace(' ','',regex=True)
        db_content['jou_title_id_str']= db_content['jou_title_id_str']+"_"+db_content['title_id_str']   

        solr_content['jou_title_id_str'] = db_content['jou_title_id_str']

    if 'issn-type' in data_list.columns :
        db_content['issn-type'] =data_list["issn-type"]

    if debug == 'T' or debug == 't':
        print("Fixed fields Insertion Completed Successfully")
    return solr_content,db_content,columns

def crossrefChangeField(data_list,columns,solr_content,db_content,debug,jid):
    err="blank"
    clean = re.compile('<.*?>')
    pissn, eissn, author, author2, dateSpan, abstract ,abstract_full  = [],[],[],[],[],[],[]
    issn_type, lang, format_static = [],[],[]
    jid_str = []
    author_original,link= [],[]
    author_corporate,author_corporate_role=[],[]
    metadata_source,update_to=[],[]
    #url_ss,url_tm_ss,url_sc_ss,pubNm=[],[],[],[]
  
    for i in range(0,len(data_list.index)):
        jid_str.append(jid)
        metadata_source.append([{ "source" :"crossref","date":created_date}])
        try:            
            if 'update-to' in data_list.columns :
                check_null_update_to=data_list["update-to"][i]
                check_null = False
                if isinstance(check_null_update_to, float):
                    check_null = math.isnan(check_null_update_to)

                if check_null_update_to == 0 or check_null == True:
                    update_to.append(None)
                    try:
                        if 'type' in data_list.columns :
                            format_static.append([data_list['type'][i]])
                    except:
                        err="Error inside type field"
                else: 
                    update_data=pd.DataFrame(data_list["update-to"][i])
                    format_static.append([update_data['type'][0]])

                    #mongoDB
                    update_to.append([{"DOI":update_data['DOI'][0]}])
            else:
                try:
                    if 'type' in data_list.columns :
                        format_static.append([data_list['type'][i]])
                except:
                    err="Error inside type field"
        except:
            err="Error inside update-to field for correction type"
 
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
                        abstract_full.append(None)
                    else:   
                        abstract.append(re.sub(clean, '', str(data_list['abstract'][i])))
                        abstract_full.append(data_list['abstract'][i])
                else:
                    abstract.append(None)
                    abstract_full.append(None)
        except:
            err="Error inside abstract field"

        try:            
            if 'language' in data_list.columns :
                check_null_lang=data_list["language"][i]
                check_null = False

                if isinstance(check_null_lang, float):
                    check_null = math.isnan(check_null_lang)
                
                if check_null_lang == 0 or check_null == True:
                    lang.append(None)
                else:   
                    lang.append(p[str(data_list['language'][i])].data)
        except:
            err="Error inside language field"

        try:          
            if 'author' in data_list.columns :
                check_null_author=data_list["author"][i]
                check_null = False

                if isinstance(check_null_author, float):
                    check_null = math.isnan(check_null_author)
                
                if check_null_author == 0 or check_null == True:
                    author.append(None)
                    author2.append(None)
                    author_original.append(None)
                    author_corporate.append(None)
                    author_corporate_role.append(None)
                else:  
                    #author_data_i = pd.DataFrame(literal_eval(data_list["author"][i]))
                    author_data_i = pd.DataFrame(data_list["author"][i])
                    # print(author_data_i)
                    # print("-----------------")
                    #for Mongodb
                    if 'ORCID' in author_data_i.columns :
                        author_data_i.drop(['ORCID'],axis=1,inplace=True)
                    if "authenticated-orcid" in author_data_i.columns:
                        author_data_i.drop(['authenticated-orcid'],axis=1,inplace=True)

                    if ("given" in author_data_i.columns) and("family" in author_data_i.columns):
                        author_data_i["author_fullnm"]=author_data_i["given"]+" "+author_data_i["family"]
                    elif ("given" not in author_data_i.columns) and ("family" in author_data_i.columns):
                        author_data_i["author_fullnm"]=author_data_i["family"]
                    elif ("given" in author_data_i.columns) and ("family" not in author_data_i.columns):
                        author_data_i["author_fullnm"]=author_data_i["given"]

                    author_original.append(json.loads(author_data_i.to_json(orient='records')))
                                
                    #For Solr
                    row_no=0
                    if ("name" in author_data_i.columns):
                        #print(author_data_i[author_data_i['name'].notnull()])
                        author_name = author_data_i[author_data_i['name'].notnull()]
                        name_index = author_name.index[0]
                        author_corporate.append(author_name['name'].values[0]) 
                        author_corporate_role.append("Group Author")
                        author_data_i.drop([name_index],inplace=True)
                    else:
                        author_corporate.append(None)
                        author_corporate_role.append(None)

                    if (len(author_data_i) > row_no):
                        author_data_i_0 = author_data_i.iloc[[row_no]]
                        if ('given' in author_data_i_0.columns) and ('family' in author_data_i_0.columns)  :
                            author.append(((author_data_i_0['given'].astype(str)+' '+author_data_i_0['family'].astype(str)).tolist()))   
                        elif 'given' not in author_data_i_0.columns and 'family' in author_data_i_0.columns  :
                            author.append(((author_data_i_0['family'].astype(str)).tolist()))   
                        elif 'given' in author_data_i_0.columns and 'family' not in author_data_i_0.columns  :
                            author.append(((author_data_i_0['given'].astype(str)).tolist())) 
                        elif ('given' not in author_data_i_0.columns) and ('family' not in author_data_i_0.columns)  :                
                            author.append(None)
                    else:
                        author.append(None)

                    #Except first record       
                    if (len(author_data_i) > row_no+1):
                        author_data_i_1_2=author_data_i.iloc[row_no+1:]
                        if ('given' in author_data_i_1_2.columns) and ('family' in author_data_i_1_2.columns)  :
                            author2.append(((author_data_i_1_2['given'].astype(str)+' '+author_data_i_1_2['family'].astype(str)).tolist()))   
                        elif 'given' not in author_data_i_1_2.columns and 'family' in author_data_i_1_2.columns  :
                            author2.append(((author_data_i_1_2['family'].astype(str)).tolist()))   
                        elif 'given' in author_data_i_1_2.columns and 'family' not in author_data_i_1_2.columns  :
                            author2.append(((author_data_i_1_2['given'].astype(str)).tolist()))  
                        elif ('given' not in author_data_i_1_2.columns) and ('family' not in author_data_i_1_2.columns) :
                            author2.append(None)
                    else:
                        author2.append(None)
        except:
            err="Error inside author field"
        
        try:          
            if 'link' in data_list.columns :
                check_null_link_data=data_list["link"][i]
                check_null = False
                
                if isinstance(check_null_link_data, float):
                    check_null = math.isnan(check_null_link_data)

                if check_null_link_data == 0 or check_null == True:
                    link.append(None)
                else:
                    #For MongoDB
                    link_list=data_list["link"].iloc[i]
                    for index in range(len(link_list)):
                        if "intended-application" in link_list[index]:
                            link_list[index]["location"] = str("crossref_"+str(link_list[index]["intended-application"]))
                            del link_list[index]["intended-application"]
                        if "content-version" in link_list[index]:
                            del link_list[index]["content-version"]
                       
                    link.append(link_list)
                # if check_null_link_data == 0 or check_null == True:
                #     url_ss.append(None)
                #     url_sc_ss.append(None)
                #     url_tm_ss.append(None)
                # else:
                #     single_link_data = check_null_link_data
                #     if len(single_link_data) > 0 :
                #         u_ss ,u_tm_ss,u_sc_ss=[],[],[]
                #         link_data=pd.DataFrame(single_link_data)
                #         for k in range (0,len(link_data)):
                #             if link_data["intended-application"][k] == "text-mining":
                #                 u_tm_ss.append(link_data["URL"][k])
                #             elif link_data["intended-application"][k] == "similarity-checking":
                #                 u_sc_ss.append(link_data["URL"][k])
                #             else:
                #                 u_ss.append(link_data["URL"][k])
                #         if len(u_ss) == 0:
                #             url_ss.append(None)
                #         else:
                #             url_ss.append(u_ss)
                #         if len(u_sc_ss) == 0:
                #             url_sc_ss.append(None)
                #         else:
                #             url_sc_ss.append(u_sc_ss)
                #         if len(u_tm_ss) == 0:
                #             url_tm_ss.append(None)
                #         else:
                #             url_tm_ss.append(u_tm_ss)       
        except:
            err="Error inside link field"
       
        try:            
            if 'issn-type' in data_list.columns :
                check_null_issn_type=data_list["issn-type"][i]
                check_null = False
                if isinstance(check_null_issn_type, float):
                    check_null = math.isnan(check_null_issn_type)

                if check_null_issn_type == 0 or check_null == True:
                    # if debug == 'T' or debug == 't':
                    #     print("Issn-type is empty")
                    pissn.append(None)
                    eissn.append(None)                        
                else: 
                    ps= False
                    es= False
                    ref=pd.DataFrame(data_list["issn-type"][i]).fillna(0)
                    for x in range(0,len(ref)):
                        if ref['type'][x] == "print":
                            if ps == False:
                                pissn.append(ref['value'][x])
                                ps= True
                            else:
                                ErrorInAnyFieldAddDataInessCheckDataSQL(data_list['DOI'][i],data_list['source'][i],'More than 1 Print Issn')
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
            if 'title' in data_list.columns :
                title_len=data_list["title"][i][0]
                if len(title_len.encode('utf-8')) >=1000:
                    print("title lenth is more than 1000")
                    ErrorInAnyFieldAddDataInessCheckDataSQL(data_list['DOI'][i],data_list['source'][i],'>1000 len of title')
        except:
            err="Error inside title field"

        try:
            if 'published-print' in data_list.columns :
                pubPrintnull = pd.isnull(data_list["published-print"][i])
                if pubPrintnull == True:
                    published_date = 0 
                else:
                    published_date=data_list["published-print"][i]
                    
                if published_date == 0 or (len(published_date.keys())==0):
                    if 'published-online' in data_list.columns :
                        pubOnlinenull = pd.isnull(data_list["published-online"][i])
                        if pubOnlinenull == True:
                            published_dateo = 0 
                        else:
                            published_dateo=data_list["published-online"][i]

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
                                            
        try:
            if 'container-title' in data_list.columns :
                container_title_len=data_list['container-title'].str.len()
                if container_title_len[i] >1 :
                    ErrorInAnyFieldAddDataInessCheckDataSQL(data_list['DOI'][i],data_list['source'][i],'More than 1 Container Title')
        except:
            err="Error inside container-title field"       
    
    try:
        if 'author' in data_list.columns :    
            solr_content['author']=pd.Series(author)
            solr_content['author2']=pd.Series(author2) 
            solr_content['author_corporate']=pd.Series(author_corporate) 
            solr_content['author_corporate_role']=pd.Series(author_corporate_role) 
            columns.append('author')
            columns.append('author2')
            db_content["author"] = author_original
            print("upto author completed") 

        solr_content['publishDate']= dateSpan  
        solr_content['publishDateSort']= dateSpan
        db_content['publishDate']=solr_content['publishDate']  
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
            db_content['abstract']=abstract_full
            solr_content['description_full_str'] = abstract_full
            columns.append('description')
            print("upto description completed") 

        if len(format_static)>0:   
            solr_content['format']= format_static
            print("upto format completed") 

        if len(update_to)>0:  
            db_content['update-to'] = update_to
            print("upto update-to completed") 

        if 'language' in data_list.columns :   
            solr_content['language'] = lang
            db_content['language'] = lang
            print("upto language completed") 
        
        if len(link)>0:
            db_content['link'] = link

        # if len(url_sc_ss)>0:
        #     solr_content['url_sc_str_mv']= url_sc_ss
        #     print("upto url_sc_str_mv completed") 

        # if len(url_tm_ss)>0:
        #     solr_content['url_tm_str_mv']= url_tm_ss
        #     print("upto url_tm_str_mv completed")     

        # if len(url_ss)>0:
        #     solr_content['url_str_mv']= url_ss
        #     print("upto url_str_mv completed") 

        #Static fields
        solr_content['jid_str']= jid_str
        db_content['journal_id']= jid_str
        db_content['metadata_source']= metadata_source
    except Exception as e:
        err = e

    if err == "blank":
        print("Change Fields Insertion Completed")
    else:
        print("error inside change fiedls:"+str(err))
    return solr_content,db_content,columns,err

# def checkDataAvilable(jid,frmyear,toyear) : 
#     frmyear=int(frmyear)
#     toyear=int(toyear)
#     todays_date = date.today()
#     multipalyrlst,insertyr =[],[]
#     if frmyear != toyear:
#         if toyear == 9999 :
#             for i in range(frmyear,todays_date.year+1):
#                 multipalyrlst.append(frmyear)
#                 frmyear = frmyear+1
#         else:
#             for i in range(frmyear,toyear+1):
#                 multipalyrlst.append(frmyear)
#                 frmyear = frmyear + 1
#     else:
#         multipalyrlst.append(str(frmyear))

#     print("yearlist "+str(multipalyrlst)) 

#     for i in multipalyrlst:
#         year = str(i)
#         sql_select_Query = "select * from ess_inserteddata where journalId='"+str(jid)+"' and year='"+year+"'"
#         cursor.execute(sql_select_Query)
#         data = cursor.fetchall()
#         if len(data) == 0 or len(data) == -1:
#            insertyr.append(year)
#         elif len(data) == 1:
#             print("Data already available for year "+str(year)+" in database")
#         #print("insertyr ::"+str(insertyr))
#     return insertyr   

# def addDataInAllIdSQL(id_list,id_check,jid):
#     pub_nm_id_Query="select id from publisher_name where publisher_name=(SELECT publisher_name FROM `survey_ejournals` WHERE id='"+str(jid)+"')"
#     cursor.execute(pub_nm_id_Query)
#     pub_nm_id = cursor.fetchall()
#     for pid in pub_nm_id:
#         p_id=pid[0]

#     id_table= pd.DataFrame({'ejournal_id' : []})
#     id_table['pub_raw_id'] = id_list
#     id_not_in = (~id_table['pub_raw_id'].isin(id_check))
#     id_table['ejournal_id'] = jid
#     id_table['pub_name_id'] = p_id
#     id_table[id_not_in].to_sql('ess_allid', con = engine, index=False ,if_exists='append') 
#     print("Data inserted in All id table in mysql")
