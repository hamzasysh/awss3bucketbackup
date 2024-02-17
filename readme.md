
following is the command to run this script


python script.py mongodburi  C:\Users\HF\Desktop\db-dump\gladiator-base\scripts.bson,C:\Users\HF\Desktop\db-dump\gladiator-base\customer_enhanced.bson mdexam-storage-bucket 2024-02-16_4/gladiator-base/scripts/scripts.bson:2024-02-16_4/gladiator-base/customer_enhanced/customer_enhanced.bson C:\Users\HF\Desktop\db-dump\s3-gladiator-base\scripts.bson,C:\Users\HF\Desktop\db-dump\s3-gladiator-base\customer_enhanced.bson mongodburi awss3


now we should arguments passed,

1. python script.py ( python keyword + script name)
2. mongodb-uri (mongodb db from which you want to dump data)
3. collections (colon seperated collections you want to dump)
4. paths of local machine where you want to dump (comma seperated paths)
5. s3 bucket name
6. object/key of collections in s3 bucket (colon seperated keys)
7. paths of location where you copied collections from s3 to local machine (comma seperated paths)
8. mongodburi (uri of mongodb where you want to restore bucket collections)
9. database (name of database in the target mongodb cluster)