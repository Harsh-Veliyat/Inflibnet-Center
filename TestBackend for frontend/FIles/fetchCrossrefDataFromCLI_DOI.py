from habanero import Crossref
from CrossRefDataInsertSolrMongoDB import *
from commonFunction import fetchJournalID
import sys

update = 'h'

if len(sys.argv) == 1:
    print("Please Insert DOI")
    sys.exit()
elif len(sys.argv) == 2:
    doi=sys.argv[1]
elif len(sys.argv) == 3:
    doi=sys.argv[1]
    update=sys.argv[2]

# Crossref
cr = Crossref(mailto = "eshodhsindhu@inflibnet.ac.in")
cr.journals()
w1 = cr.works(ids=doi)
    
if type(w1) == dict:
    issn = w1["message"]["ISSN"]
else:
    dic_tot = w1[0]
    issn = dic_tot["message"]["ISSN"]
   
print("issn: "+str(issn))

# Check Journal Id is availbale on DB or not
jid,issnl = fetchJournalID(issn)

print("JournalId: "+str(jid))
print("ISSN-L: "+str(issnl))

retrieveCrossrefStoreDBSolr(doi,jid,issnl,w1,'***','t','***','***',update,'***')

