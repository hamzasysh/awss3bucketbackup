import subprocess
import os
import boto3
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(filename="app.log", level=logging.INFO)


def mongodump(uri, outpath):
    command = [
        "mongodump",
        "--uri",
        uri,
        "--out",
        outpath,
    ]
    try:
        subprocess.run(command, check=True)
        logging.info("Backup completed successfully.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Backup failed: {e}")


def uploadtos3(outpath, bucket, max_backups):
    s3 = boto3.client("s3")
    current_date = datetime.now().date()
    path = None
    datestr = None

    try:
        for i in range(1, 5):
            temp = s3.list_objects_v2(Bucket=bucket, Prefix=f"{current_date}_{i}")
            if "Contents" in temp:
                datestr = f"{current_date}_{i}"
        if datestr:
            no = int(datestr.split("_")[1])
            if no:
                if no == max_backups:
                    logging.error(
                        "Today's Backups are already stored. Come Back tomorrow"
                    )
                    return
                path = f"{current_date}_{no + 1}"
            else:
                path = f"{current_date}_1"
        else:
            path = f"{current_date}_1"
        for root, _, files in os.walk(outpath):
            if (root == "admin") or (root == "config") or (root == "local"):
                continue
            for file in files:
                local_path = os.path.join(root, file)
                if file.endswith(".metadata.json"):
                    key = (
                        f"{path}/{os.path.split(root)[-1]}/{file.split('.')[0]}/{file}"
                    )
                else:
                    key = f"{path}/{os.path.split(root)[-1]}/{os.path.splitext(file)[0]}/{file}"
                s3.upload_file(local_path, bucket, key)
                logging.info(f"File uploaded successfully: {key}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")


def download_from_s3(bucket, folder, s3objpath):
    s3 = boto3.client("s3")
    try:
        objects = s3.list_objects_v2(Bucket=bucket, Prefix=folder)["Contents"]
        for obj in objects:
            key = obj["Key"]
            if not os.path.exists(
                (os.path.join(s3objpath, key.split("/")[0], key.split("/")[1]))
            ):
                os.makedirs(
                    os.path.join(s3objpath, key.split("/")[0], key.split("/")[1])
                )
            with open(
                os.path.join(
                    s3objpath, key.split("/")[0], key.split("/")[1], key.split("/")[3]
                ),
                "wb",
            ) as f:
                s3.download_fileobj(bucket, key, f)
            logging.info("file with key: " + key + " downloaded from s3")
    except Exception as e:
        logging.error(f"Backup failed: {e}")


def mongorestore(uri, rfolder):
    try:
        command = ["mongorestore", "--uri", uri, rfolder]
        subprocess.run(command, check=True)
        logging.info("Restore completed successfully.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Restore failed: {e}")


def cleanup_backups(bucket):
    s3 = boto3.client("s3")
    four_days_ago = datetime.now() - timedelta(days=4)
    four_days_ago = four_days_ago.date()
    try:
        for i in range(1, 4):
            folder = s3.list_objects_v2(Bucket=bucket, Prefix=f"{four_days_ago}_{i}")
            if "Contents" in folder:
                boto3.resource("s3").Bucket(bucket).objects.filter(
                    Prefix=f"{four_days_ago}_{i}"
                ).delete()
            else:
                logging.info(f"No backup exist for date {four_days_ago}_{i}.")
    except Exception as e:
        logging.error(f"Backup cleaning failed: {e}")


if __name__ == "__main__":

    source_uri = os.getenv("source_uri")
    outpath = os.getenv("outpath")
    bucket = os.getenv("bucket")
    folder = os.getenv("folder")
    s3objpath = os.getenv("s3objpath")
    destination_uri = os.getenv("destination_uri")
    max_backups = os.getenv("max_backups")
    rfolder = os.getenv("restorefolder")

    mongodump(source_uri, outpath)
    cleanup_backups(bucket)
    uploadtos3(outpath, bucket, max_backups)
    download_from_s3(bucket, folder, s3objpath)
    mongorestore(destination_uri, rfolder)
