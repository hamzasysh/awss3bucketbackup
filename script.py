import subprocess
import os
import boto3
import logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(filename="app.log", level=logging.INFO)


def mongodump(database, outpath):
    command = [
        "mongodump",
        "--uri",
        database,
        "--out",
        outpath,
    ]
    try:
        subprocess.run(command, check=True)
        logging.info("Backup completed successfully.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Backup failed: {e}")


def uploadtos3(outpath, bucket):
    s3 = boto3.client("s3")
    current_date = datetime.now().date()
    path = None
    datestr = None

    try:
        for i in range(1, 8):
            temp = s3.list_objects_v2(Bucket=bucket, Prefix=f"{current_date}_{i}")
            if "Contents" in temp:
                datestr = f"{current_date}_{i}"
        if datestr:
            no = int(datestr.split("_")[1])
            if no:
                path = f"{current_date}_{no + 1}"
            else:
                path = f"{current_date}_1"
        else:
            path = f"{current_date}_1"

        for root, _, files in os.walk(outpath):
            for file in files:
                local_path = os.path.join(root, file)
                s3_path = f"{path}/{file}"
                s3.upload_file(local_path, bucket, s3_path)
                logging.info(f"File uploaded successfully: {s3_path}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")


def download_from_s3(bucket, folder, s3objpath):
    s3 = boto3.client("s3")
    try:
        objects = s3.list_objects_v2(Bucket=bucket, Prefix=folder)["Contents"]
        for obj in objects:
            key = obj["Key"]
            with open(os.path.join(s3objpath, obj["Key"].split("/")[1]), "wb") as f:
                s3.download_fileobj(bucket, key, f)
            logging.info("file with key: " + key + " downloaded from s3")
    except Exception as e:
        logging.error(f"Backup failed: {e}")


def mongorestore(uri, s3objpath):
    command = [
        "mongorestore",
        "--uri",
        uri,
        s3objpath,
    ]
    try:
        subprocess.run(command, check=True)
        logging.info("Restore completed successfully.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Restore failed: {e}")


if __name__ == "__main__":

    source_uri = os.getenv("source_uri")
    outpath = os.getenv("outpath")
    bucket = os.getenv("bucket")
    folder = os.getenv("folder")
    s3objpath = os.getenv("s3objpath")
    destination_uri = os.getenv("destination_uri")

    mongodump(source_uri, outpath)
    uploadtos3(outpath, bucket)
    download_from_s3(bucket, folder, s3objpath)
    mongorestore(destination_uri, s3objpath)
