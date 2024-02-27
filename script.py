import subprocess
import os
import boto3
import argparse
import logging
from ProgressPercentage import ProgressPercentage
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(filename=os.getenv("log_file_path"), level=logging.INFO)


def mongodump(uri, outpath, log_file):
    command = [
        "mongodump",
        "--uri",
        uri,
        "--out",
        outpath,
    ]
    try:
        with open(log_file, "a") as f:
            logging.info("Backup started successfully.")
            result = subprocess.run(
                command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            f.write(result.stdout.decode("utf-8"))
            f.write(result.stderr.decode("utf-8"))
            logging.info("Backup completed successfully.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Backup failed: {e}")
        
    return


def uploadtos3(outpath, bucket, max_backups, log_file):
    s3 = boto3.client("s3")
    current_date = datetime.now().date()
    path = None
    datestr = None

    try:
        for i in range(1, max_backups + 1):
            temp = s3.list_objects_v2(Bucket=bucket, Prefix=f"{current_date}_{i}")
            if "Contents" in temp:
                datestr = f"{current_date}_{i}"
        if datestr:
            no = int(datestr.split("_")[1])
            if no == max_backups:
                logging.error("Today's Backups are already stored. Come Back tomorrow")
                return
            path = f"{current_date}_{no + 1}"
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
                s3.upload_file(
                    local_path,
                    bucket,
                    key,
                    Callback=ProgressPercentage(local_path, log_file),
                )
                logging.info(f"File uploaded successfully: {key}")
        return path
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    return

def download_from_s3(bucket, folder, s3objpath, log_file):
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
    return

def mongorestore(uri, rfolder, log_file):
    try:
        command = ["mongorestore", "--uri", uri, rfolder]
        with open(log_file, "a") as f:
            logging.info("Restore started successfully.")
            result = subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            f.write(result.stdout.decode("utf-8"))
            f.write(result.stderr.decode("utf-8"))
            logging.info("Restore completed successfully.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Restore failed: {e}")
    return

def cleanup_backups(bucket, go_back_n_days):
    s3 = boto3.client("s3")
    days_ago = datetime.now() - timedelta(days=go_back_n_days)
    days_ago = days_ago.date()
    try:
        i = 1
        while "Contents" in s3.list_objects_v2(
            Bucket=bucket, Prefix=f"{days_ago}_{i+1}"
        ):
            boto3.resource("s3").Bucket(bucket).objects.filter(
                Prefix=f"{days_ago}_{i}"
            ).delete()
            logging.info(f"backup deleted for date {days_ago}_{i}.")
            i += 1
        logging.info("clean backup operation completed successfully")
    except Exception as e:
        logging.error(f"Backup cleaning failed: {e}")
    return

def get_folder(bucket_name):
    s3 = boto3.client("s3")

    # List objects in the bucket
    response = s3.list_objects_v2(Bucket=bucket_name)

    # Extract folder names from object keys
    folder_names = set()
    if "Contents" in response:
        for obj in response["Contents"]:
            key = obj["Key"]
            # Extract folder name
            folder_name = key.split("/")[0]
            if folder_name:
                folder_names.add(folder_name)
    folder_names = sorted(
        folder_names,
        key=lambda x: (x.split("_")[0], int(x.split("_")[1])),
        reverse=True,
    )
    logging.info(f"latest folder for backup : {folder_names[0]}")
    return folder_names[0]

def main():
    parser = argparse.ArgumentParser(description="AWS S3 & MongoDB Script")
    parser.add_argument(
        "--backup", action="store_true", help="Perform backup operation"
    )
    parser.add_argument(
        "--restore", action="store_true", help="Perform restore operation"
    )
    parser.add_argument(
        "--cleanup", action="store_true", help="Perform cleanup operation"
    )
    parser.add_argument("folder", type=str, nargs="?", help="Path to the folder")

    args = parser.parse_args()

    try:
        source_uri = os.getenv("source_uri")
        outpath = os.getenv("outpath")
        bucket = os.getenv("bucket")
        s3objpath = os.getenv("s3objpath")
        destination_uri = os.getenv("destination_uri")
        max_backups = int(os.getenv("max_backups"))
        log_file = os.getenv("log_file_path")
        go_back_n_days = int(os.getenv("go_back_n_days"))
    except Exception as e:
        print("Error in getting variables from env", e)
        return
        
    folder = None
    rfolder = None

    if args.backup:
        try:
            mongodump(source_uri, outpath, log_file)
            uploadtos3(outpath, bucket, max_backups, log_file)
        except Exception as e:
            print("Error in backup", e)
            return
    elif args.restore:
        try:
            if args.folder is not None:
                download_from_s3(bucket, args.folder, s3objpath, log_file)
                rfolder = os.path.join(s3objpath, args.folder)
                mongorestore(destination_uri, rfolder, log_file)
            else:
                folder = get_folder(bucket)
                download_from_s3(bucket, folder, s3objpath, log_file)
                rfolder = os.path.join(s3objpath, folder)
                mongorestore(destination_uri, rfolder, log_file)
        except Exception as e:
            print("Error in restore", e)
            return
    elif args.cleanup:
        try:
            cleanup_backups(bucket, go_back_n_days)
        except Exception as e:
            print("Error in cleanup_backups", e)
            return
    else:
        try:
            mongodump(source_uri, outpath, log_file)
            folder = uploadtos3(outpath, bucket, max_backups, log_file)
            download_from_s3(bucket, folder, s3objpath, log_file)
            rfolder = os.path.join(s3objpath, folder)
            mongorestore(destination_uri, rfolder, log_file)
            try:
                cleanup_backups(bucket, go_back_n_days)
            except Exception as e:
                print("Error in cleanup backup because", e)
        except Exception as e:
            print("Error in dumping and restoring data to AWS because: ", e)

    return
    
    
if __name__ == "__main__":
    main()
    