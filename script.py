import subprocess
import sys
import boto3

def mongodump(database,collections,outpaths):
     for i in range(len(collections)):
          command = ["mongodump", "--uri", database, "--collection", collections[i],"--out",outpaths[i]]
          try:
              subprocess.run(command, check=True)
              print("Backup completed successfully.")
          except subprocess.CalledProcessError as e:
              print(f"Backup failed: {e}")

def uploadtos3(outpaths,bucket,obj_paths):
    try:
        s3 = boto3.client('s3')
    except Exception as e:
        print(f"s3 bucket client failed: {e}")

    for i in range(len(outpaths)):
        try:
            with open(outpaths[i], 'rb') as f:
                s3.upload_fileobj(f, bucket, obj_paths[i])
            print(f"File uploaded successfully to S3: s3://{bucket}/{obj_paths[i]}")
        except Exception as e:
            print(f"File upload to S3 failed: {e}")

def download_from_s3(bucket, obj_paths, s3objpaths):
    # Initialize the S3 client
    try:
        s3 = boto3.client('s3')
    except Exception as e:
        print(f"s3 bucket client failed: {e}")

    for i in range(len(obj_paths)):
        try:
            with open(s3objpaths[i], 'wb') as f:
                s3.download_fileobj(bucket, obj_paths[i], f)
            #s3.download_file(bucket, obj_paths[i],s3objpaths[i])
            print(f"File downloaded successfully to: {s3objpaths[i]}")
        except Exception as e:
            print(f"File download from S3 failed: {e}")


def mongorestore(uri,database2,collections,s3objpaths):
    for i in range(len(collections)):
        command = ["mongorestore", "--uri", uri, "--nsInclude", database2+"."+collections[i],s3objpaths[i]]
        print(command)
        try:
            subprocess.run(command, check=True)
            print("Backup completed successfully.")
        except subprocess.CalledProcessError as e:
            print(f"Backup failed: {e}")


if __name__ == "__main__":

    database = sys.argv[1]
    collections = sys.argv[2].split(":")
    outpaths=sys.argv[3].split(",")
    bucket=sys.argv[4]
    obj_paths=sys.argv[5].split(":")
    s3objpaths=sys.argv[6].split(",")
    uri=sys.argv[7]
    database2=sys.argv[8]


    mongodump(database, collections,outpaths)
    uploadtos3(outpaths,bucket,obj_paths)
    download_from_s3(bucket, obj_paths, s3objpaths)
    mongorestore(uri,database2,collections,s3objpaths)

