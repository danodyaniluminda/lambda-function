import os
import datetime

efs_mount_path = "/DTE_app_logs"
days_threshold_delete = 7

def lambda_handler(event, context):
    try:
        for entry in os.scandir(efs_mount_path):
            if entry.is_dir() and entry.name == "logs":
                process_directory(entry.path)
        return {
            'statusCode': 200,
            'body': 'Old log and gz files deleted successfully.'
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

def delete_old_file(file_path):
    os.remove(file_path)
    print(f"Deleted: {file_path}")

def process_directory(directory_path):
    for entry in os.scandir(directory_path):
        if entry.is_file() and (entry.name.endswith('.gz') or entry.name.endswith('.log')):
            if calculate_time_difference(entry.path) >= days_threshold_delete:
                delete_old_file(entry.path)
        elif entry.is_dir():
            process_directory(entry.path)
