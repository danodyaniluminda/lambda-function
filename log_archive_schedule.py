import os
import datetime
import gzip
import shutil

efs_mount_path = "/mnt/efs-log-1c"
days_threshold_gz = 2

def lambda_handler(event, context):
    try:
        for entry in os.scandir(efs_mount_path):
            if entry.is_dir() and entry.name == "logs":
                process_directory(entry.path)
        return {
            'statusCode': 200,
            'body': 'Old log files gzipped successfully.'
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }

def calculate_time_difference(file_path):
    current_time = datetime.datetime.now()
    file_creation_time = datetime.datetime.fromtimestamp(os.path.getctime(file_path))
    return (current_time - file_creation_time).days

def gzip_and_remove(file_path):
    file_name = os.path.basename(file_path)
    gzipped_file_path = file_path + '.gz'
    with open(file_path, 'rb') as f_in, gzip.open(gzipped_file_path, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
    os.remove(file_path)
    print(f"File '{file_name}' was gzipped and removed (older than {days_threshold_gz} days).")

def process_directory(directory_path):
    for entry in os.scandir(directory_path):
        if entry.is_file():
            time_difference = calculate_time_difference(entry.path)
            if entry.name.endswith('.log') and time_difference >= days_threshold_gz:
                gzip_and_remove(entry.path)
        elif entry.is_dir():
            process_directory(entry.path)
