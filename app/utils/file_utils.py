import os

def ensure_path_exists(path: str):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

def get_file_name(path: str):
    return os.path.basename(path)

def remove_path_from_path(complete_path: str, path_to_remove: str):
    path_to_remove_with_sep = path_to_remove + os.sep if path_to_remove[-1] != os.sep else path_to_remove

    return complete_path.replace(path_to_remove_with_sep, '')

def get_all_files_in_dir(dir) -> list[str]:
    all_files = []
    for entry in os.scandir(dir):
        if entry.is_dir():
            all_files += get_all_files_in_dir(entry.path)
        else:
            all_files.append(entry.path)
    
    return all_files


def ensure_sep_at_end(path: str):
    return path + os.sep if path[-1] != os.sep else path


def remove_sep_from_start(path: str):
    return path[1:] if path[-1] == os.sep else path


def human_readable_size(size_in_bytes: int):
    if size_in_bytes == 0:
        return "0 B"
    
    # Define as unidades de medida
    units = ["B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"]
    power = 1024  # 1 KB = 1024 Bytes
    n = 0
    
    # Divida o valor até chegar na unidade adequada
    while size_in_bytes >= power and n < len(units) - 1:
        size_in_bytes /= power
        n += 1
    
    # Retorna o valor formatado com 2 casas decimais
    return f"{size_in_bytes:.2f} {units[n]}"


