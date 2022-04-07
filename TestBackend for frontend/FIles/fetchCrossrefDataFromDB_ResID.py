import sys,mysql.connector
from CrossRefDataInsertSolrMongoDB import *
from commonFunction import fetchJournalID

update = 'h'
if len(sys.argv) == 6:
    from_pub_date = sys.argv[2]
    until_pub_date = sys.argv[3]
    method = sys.argv[4]
    update = sys.argv[5]
if len(sys.argv) == 5:
    from_pub_date = sys.argv[2]
    until_pub_date = sys.argv[3]
    method = sys.argv[4]
elif len(sys.argv) == 4:
    from_pub_date=sys.argv[2]
    until_pub_date=sys.argv[3]
    print("Please Insert Method ex.CLI,Cronjob")
    sys.exit()
elif len(sys.argv) == 3:
   print("Please Insert To Year")
   sys.exit()
elif len(sys.argv) == 2:
    print("Please Insert From Year And To Year")
    sys.exit()
elif len(sys.argv) == 1:
    print("Please Insert RES ID")
    sys.exit()

print ("RES ID::"+str(sys.argv[1])+"::YEAR:::"+str(from_pub_date))

commonFunction.cursor.execute("SELECT CASE WHEN online_identifier =' ' THEN print_identifier ELSE online_identifier END as issn FROM `survey_ejournals` WHERE res_id = '"+str(sys.argv[1])+"'")
result = commonFunction.cursor.fetchall() 

if len(result) == 0:
    print("Journal Not Found in survey_ejournals Table")
else:
    for res in result:
        # print(res)
        for issn_id in res:
            # print(issn_id)
            # Check Journal Id is availbale on DB or not
            jid,issnl = fetchJournalID(issn_id)

            # # Check data is inserted or not
            # insertyr = checkDataAvilable(jid,from_pub_date,until_pub_date)

            ans=commonFunction.cr.journals(ids=issn_id,works=True,filter={'from-pub-date' : from_pub_date,'until-pub-date':until_pub_date ,'type': 'journal-article'},facet = 'published:*')
            tot_records=ans['message']["total-results"]
            #print("Total records "+str(tot_records))
            if type(ans) == dict:
                year_list=ans["message"]["facets"]["published"]["values"]
            else:
                year_list=ans[0]["message"]["facets"]["published"]["values"]

            if tot_records>10000 and from_pub_date == until_pub_date:
                try:
                    # First time cursor value is * other wise last cursor value
                    cmd_cursor = input("Enter Cursor:")
                except:
                    print("Please Enter Cursor Value")
                try:
                    # First time last_end_no value is 0 other wise last_end_no like 8988 when code stop
                    last_end_no = int(input("Enter Last End Value:"))
                except:
                    print("Please Enter Last End Value")

                # Here 't' for print msg in console 
                retrieveCrossrefStoreDBSolr(issn_id,jid,issnl,from_pub_date,until_pub_date,'t',cmd_cursor,last_end_no,update,method)
                
            elif tot_records>10000 and from_pub_date!= until_pub_date:
                print("Please Select Single Year Total Records are more than 10000")

            else:
                retrieveCrossrefStoreDBSolr(issn_id,jid,issnl,from_pub_date,until_pub_date,'t','**','**',update,method)

            
            

               

