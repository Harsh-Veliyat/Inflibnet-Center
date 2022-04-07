import sys, json
import pandas as pd
from datetime import datetime
import commonFunction

def retrieveCrossrefStoreDBSolr(issn_id,jid,issnl,from_pub_date,until_pub_date,debug,cmd_cursor,last_end_no,full_update,method):
    #start = offset_cmd
    if cmd_cursor !="**" and cmd_cursor!="***":
        next_cursor = cmd_cursor
        #cnt = last_end_no
        start = last_end_no
        max_cursor = 100
    else:
        start=0
    errm,msg="",""
    try:
        while 1:
            try:
                if cmd_cursor == "**" and last_end_no == "**":
                    w1  = commonFunction.cr.journals(ids=issn_id, works=True, offset=str(start), limit=100,filter={'from-pub-date' : from_pub_date,'until-pub-date':until_pub_date,'type': 'journal-article'},facet = 'published:*')
                elif cmd_cursor == "***" and last_end_no == "***":
                    # In case of doi crossref data add in from_pub_date (for use same method)
                    data_var = [from_pub_date["message"]]
                else:
                    w1=commonFunction.cr.journals(ids=issn_id, works=True, cursor=next_cursor ,limit=100,cursor_max = max_cursor ,filter={'from-pub-date' : from_pub_date,'type': 'journal-article'},facet = 'published:*' )

            except Exception as e:
                print("CrossRef Error:::::::"+str(e))
                #################
                if start == 0:
                    noOfRecords = 0
                else:
                    noOfRecords = start-100
                commonFunction.addDataInUpdatedStatusSQL(jid,"Crossref Error:"+str(e), method, noOfRecords)
                commonFunction.updateDataInUpdatedFrequencySQL(jid)
                return

            if cmd_cursor != "***" and last_end_no != "***":
                if type(w1) == dict:
                    tot=w1['message']['total-results']
                    data_var= w1["message"]["items"]
                else:
                    dic_tot=w1[0]
                    tot=dic_tot['message']['total-results']
                    data_var= dic_tot["message"]["items"]
            else:#In doi 
                tot = 1

            if tot == 0 :
                print("No Data available for in Crossref")
                ############################
                if cmd_cursor != "***":
                    commonFunction.addDataInUpdatedStatusSQL(jid,"No Data available for in Crossref", method, 0)
                return
            else :
                if debug == 'T' or debug == 't':    
                    print("Total Records "+str(tot))
                data_list = pd.DataFrame(data_var)

                #Add new line for cursor
                #data_list_len=len(data_list.index)
                #End

            if int(tot)>0:
                columns=['issn']
                solr_content = pd.DataFrame({'id' : []})
                db_content = pd.DataFrame({'DOI' : []})

                # Fixed solr field Method
                solr_content, db_content, columns = commonFunction.crossrefFixedField(data_list,columns,solr_content,db_content,debug)

                # Change solr field Method
                solr_content,db_content,columns,err = commonFunction.crossrefChangeField(data_list,columns,solr_content,db_content,debug,jid)
                errm = err
                #print("DataFrame db col:::"+str(db_content))
                # db_content.to_csv('F:/db_content1.csv')
                if errm == "blank":
                    listToStr=[str(i) for i in data_list['DOI'].tolist()]
                    db_content['issn_l']= issnl
                    db_content['lastUpdate']= commonFunction.created_date

                    check_doi_Query = commonFunction.db.publication_raw.find({"DOI": {"$in": listToStr}},{"_id": 1,"DOI": 1})
                    id_check, doi_check= [],[]
                    for dataid in check_doi_Query:
                        id_check.append(str(dataid.get('_id')).replace("'",''))
                        doi_check.append(dataid['DOI'].replace("'",''))

                    #Data frame divide in two part insert and update
                    rslt_update_db_content = db_content[db_content['DOI'].isin(doi_check)]
                    rslt_insert_db_content =  db_content[~db_content['DOI'].isin(doi_check)]
                    # print("Insert datafrmae")
                    # print(rslt_insert_db_content)
                    print("update dataframe")
                    #print(rslt_update_db_content)
                    if (check_doi_Query.count()) == 0:
                        db_content['createdDate']= commonFunction.created_date
                        db_data=db_content.to_json(orient='records')
                        db_insert = json.loads(db_data) 
                        commonFunction.db.publication_raw.insert_many(db_insert)
                    else:
                        # Update all field based on doi
                        if full_update =="f":  
                            for i, j in db_content.iterrows(): 
                                j_dict=j.to_dict()
                                j_dict["lastUpdate"]=str(commonFunction.created_date)
                                wherec = { "DOI": j_dict['DOI']}
                                newvalues={ "$set": j_dict}
                                commonFunction.db.publication_raw.update(wherec,newvalues,upsert=True)
                        else:
                            #df2 = df.loc[:, ~df.columns.isin(['Fee','Discount'])]
                            if not rslt_update_db_content.empty:
                                #rslt_update_db_content = rslt_update_db_content.loc[:, ~rslt_update_db_content.columns.isin(['link','publishDate','page','issue','volume','is-referenced-by-count'])]
                                #rslt_update_db_content=rslt_update_db_content[rslt_update_db_content.columns.difference(['link','publishDate','page','issue','volume','is-referenced-by-count'])]
                                rslt_update_db_content.drop(rslt_update_db_content.columns.difference(['DOI','link','publishDate','page','issue','volume','is-referenced-by-count']), 1, inplace=True)
                                print("del some column from update dataframe")
                                print(rslt_update_db_content)
                                for i, j in rslt_update_db_content.iterrows(): 
                                    j_dict=j.to_dict()
                                    #Fields update in mongoDB
                                    #link,publishDate,page,issue,volume,is-referenced-by-count
                                    j_dict["lastUpdate"]=str(commonFunction.created_date)
                                    wherec = { "DOI": j_dict['DOI']}
                                    newvalues={ "$set": j_dict}
                                    commonFunction.db.publication_raw.update(wherec,newvalues)

                            if not rslt_insert_db_content.empty:
                                rslt_insert_db_content['createdDate']= commonFunction.created_date
                                db_insert_data=rslt_insert_db_content.to_json(orient='records')
                                db_insert = json.loads(db_insert_data) 
                                commonFunction.db.publication_raw.insert_many(db_insert)

                    print("MongoDB insertion completed for Year")  
                
                    get_id_solr = commonFunction.db.publication_raw.find({"DOI": {"$in": listToStr}},{"_id": 1,"DOI": 1})
                    id_list,id_list_actual=[],[]
                    for x in get_id_solr:
                        id_list.append(str(x.get('_id')))
                        id_list_actual.append(x.get('_id'))

                    solr_content['id'] = id_list 
                    solr_content['allfields'] = solr_content[columns].to_dict(orient='records')
                    dfdata = pd.DataFrame(solr_content, columns= columns)
                    solr_content['allfields'] = dfdata.values.tolist() 
                    #Fields update in solr
                    #container_volume,container_start_page,container_issue,publishDate,publishDateSort 
                    
                    try:
                        if debug == 'T' or debug == 't':
                            print("Before Solr Add")
                        #http://localhost:8080/solr/biblio/select?fq=publishDate:2015&q=issn:%220003-4916%22
                        if full_update == "f":
                            solr_data=solr_content.to_json(orient='records')
                            res = json.loads(solr_data) 
                            commonFunction.solr.add(res)
                        else:
                            rslt_update_solr_content = solr_content[solr_content['doi_str_mv'].isin(doi_check)]
                            rslt_insert_solr_content =  solr_content[~solr_content['doi_str_mv'].isin(doi_check)]
                            
                            #insert
                            if not rslt_insert_solr_content.empty:
                                rslt_insert_solr_content = rslt_insert_solr_content.to_json(orient='records')
                                res = json.loads(rslt_insert_solr_content) 
                                commonFunction.solr.add(res)

                            #update
                            if not rslt_update_solr_content.empty:
                                rslt_update_solr_content.drop(rslt_update_solr_content.columns.difference(['id','container_volume','container_start_page','container_issue','publishDate','publishDateSort']), 1, inplace=True)
                                # print("solr update dataframe")
                                # print(rslt_update_solr_content)
                                rslt_update_solr_content = rslt_update_solr_content.to_json(orient='records')
                                res_update = json.loads(rslt_update_solr_content)
                                res_update_list=[]
                                for si in res_update:
                                    if "container_volume" in si:
                                        si["container_volume"]={"set": si["container_volume"]}
                                    if "container_start_page" in si:
                                        si["container_start_page"]={"set": si["container_start_page"]}
                                    if "container_issue" in si:
                                        si["container_issue"]={"set": si["container_issue"]}
                                    if "publishDate" in si:
                                        si["publishDate"]={"set": si["publishDate"]}
                                        si["publishDateSort"]= si["publishDate"]
                                    res_update_list.append(si)
                                    
                                commonFunction.solr.add(res_update_list)
                                
                        if debug == 'T' or debug == 't':
                            print("After solr Add")

                        if cmd_cursor !="**" and cmd_cursor !="***" :
                            # Start for cursor
                            if type(w1) == dict:
                                next_cursor = w1['message']['next-cursor']
                                print("next_cursor::"+str(w1['message']['next-cursor']))
                            else:
                                next_cursor = w1[0]['message']['next-cursor']
                                print("next_cursor::"+str(w1[0]['message']['next-cursor']))
                            start=start+100
                            #cnt=cnt+data_list_len
                            #end
                        else:
                            if cmd_cursor == "***":
                                #commonFunction.addDataInUpdatedStatusSQL(jid,"Successful for single doi:"+str(issn_id), method, 1)
                                #commonFunction.updateDataInUpdatedFrequencySQL(jid)
                                return
                            else:
                                start=start+100

                        msg="Data Inserted Successfully"
                        print("start:::"+str(start))
                        print("tot:::"+str(tot))

                        if cmd_cursor == "**" and last_end_no == "**":
                            if start >= tot:
                                break

                        if cmd_cursor !="**" and cmd_cursor !="***":
                            #condition for max cursor
                            if int(tot)-start>0:
                                max_cursor = min(max_cursor, int(tot)-start)
                                print("Max_Cursor::"+str(max_cursor))
                            else:
                                break  
                            #End 

                    except Exception as e:
                        commonFunction.db.publication_raw.remove({'_id':{"$in": id_list_actual}})
                        print("Solr Error Message:"+str(e))
                        print("Record deleted successfully from mongoDb due to solr error")
                        ############################## In DOI case data not insert 
                        if cmd_cursor != "***":
                            commonFunction.addDataInUpdatedStatusSQL(jid,e,method,start)
                            commonFunction.updateDataInUpdatedFrequencySQL(jid)
                        return     
                else:
                    #################################### In DOI case data not insert 
                    if cmd_cursor != "***":
                        commonFunction.addDataInUpdatedStatusSQL(jid,errm,method,start)
                        commonFunction.updateDataInUpdatedFrequencySQL(jid)
                    return

        commonFunction.addDataInUpdatedStatusSQL(jid,"Succefully",method,tot)
        commonFunction.updateDataInUpdatedFrequencySQL(jid)

    except Exception as e:
        print("Error at a time of exceutions in main try block:::::::"+str(e))
        #################################### In DOI case data not insert 
        if cmd_cursor != "***":
            commonFunction.addDataInUpdatedStatusSQL(jid,"Error inside main",method,start)
            commonFunction.updateDataInUpdatedFrequencySQL(jid)

    finally:
        print(msg)

                            # rslt_update_solr_content.drop(rslt_update_solr_content.columns.difference(['id','container_volume','container_start_page','container_issue','publishDate','publishDateSort']), 1, inplace=True)
                            #     if 'container_volume' in rslt_update_solr_content.columns :
                            #         rslt_update_solr_content['container_volume']='{"set":'+rslt_update_solr_content['container_volume']+'}'

                            #     if 'container_start_page' in rslt_update_solr_content.columns :
                            #         rslt_update_solr_content['container_start_page']='{"set":'+rslt_update_solr_content['container_start_page']+'}'
                                
                            #     if 'container_issue' in rslt_update_solr_content.columns :
                            #         rslt_update_solr_content['container_issue']='{"set":'+rslt_update_solr_content['container_issue']+'}'

                            #     if 'publishDate' in rslt_update_solr_content.columns :
                            #         rslt_update_solr_content['publishDate']='{"set":'+rslt_update_solr_content['publishDate']+'}'
                            #         rslt_update_solr_content['publishDateSort']='{"set":'+rslt_update_solr_content['publishDateSort']+'}'
                                
                            #     print("solr update dataframe")
                            #     print(rslt_update_solr_content)
                            #     rslt_update_solr_content = rslt_update_solr_content.to_json(orient='records')
            
                            #     res_update = json.loads(rslt_update_solr_content)
                            #     print(res_update)
                            #     commonFunction.solr.add(res_update)