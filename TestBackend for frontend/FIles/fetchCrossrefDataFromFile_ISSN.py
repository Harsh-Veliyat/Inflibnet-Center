import sys,csv
from CrossRefDataInsertSolrMongoDB import *
from commonFunction import validateIssn, fetchJournalID

#decided fields update only if update='f' than all fields updates
update = 'h'
issn_file_path = "E:\workspace\DiscoveryToolDiffScripts\Nlist_Crossref_Data_Fetch_46_Server\crossref\issn_no_list.txt"

with open(issn_file_path, newline = '') as file:                                                                                          
    notepad_reader = csv.reader(file, delimiter='\t')
    for data in notepad_reader:
        debug = 'f'
        if len(data) == 0:
            sys.exit()
        else:
            issn_id=data[0]
            from_pub_date=data[1]
            until_pub_date=data[2]
            method = data[3]
            update = data[4]

            # Check Inserted ISSN is valid or not
            check_issn_valid = validateIssn(str(issn_id))
            if check_issn_valid == False:
                print("Please Insert ISSN No Correct like 1234-4232 or 1234-423x or 1234-423X")
                sys.exit()

            # Check Journal Id is availbale on DB or not
            jid,issnl = fetchJournalID(str(sys.argv[1]))

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


