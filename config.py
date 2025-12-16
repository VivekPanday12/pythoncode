''' config.py for csv convertor '''
import os
import urllib.parse

sqs_queue_url = "https://sqs.ap-south-1.amazonaws.com/788384700272/nad-bulk-pdf-download.fifo"

fetch_limit = 1        # how many records to fetch per run
fetch_offset = 0     # update to 10 after first run, 20 after second run...


########### DEV DB CONFIGURATION #############
getcwd = os.path.abspath(os.curdir)
getcupload = os.path.abspath(__file__ + "/../../uploads")
mongo_uri = "mongodb://" + urllib.parse.quote("devmuser") + ":" + urllib.parse.quote("ECM87PncV24pL") + "@ip-172-22-7-171.ap-south-1.compute.internal:37017/devnad?tls=true&tlsCAFile=/opt/mongodb.pem"
db = "devnad"
org_id = '082641'
doc_type = 'DGMST'
collection_name = 'university_students_data_082641' 


########### LOCAL DB CONFIGURATION #############
# mongo_uri = "mongodb://localhost:27017" 
# db = "nad_repo" 
# org_id = '056849'
# doc_type = 'DGMST'
# collection_name = 'university_students_data'