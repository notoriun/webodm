import os

def ensure_path_exists(path: str):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

def get_file_name(path: str):
    return path.split(os.sep)[-1]

def remove_path_from_path(complete_path: str, path_to_remove: str):
    path_to_remove_with_sep = path_to_remove + os.sep if path_to_remove[-1] != os.sep else path_to_remove

    return complete_path.replace(path_to_remove_with_sep, '')
