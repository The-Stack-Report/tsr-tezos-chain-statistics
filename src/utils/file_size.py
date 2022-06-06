import os
import enum

def sizeof_fmt(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"

def get_file_size_in_bytes(file_path):
    stat_info = os.stat(file_path)
    size = stat_info.st_size
    return size


def get_file_size_readable(file_path):
    size = get_file_size_in_bytes(file_path)
    return sizeof_fmt(size)

