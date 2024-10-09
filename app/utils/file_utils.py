import os

def ensure_path_exists(path: str):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

def get_file_name(path: str):
    return path.split(os.sep)[-1]
