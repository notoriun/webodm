import os
import logging
import json

from app.utils import file_utils
from worker.utils.redis_file_cache import cache_lock

logger = logging.getLogger("app.logger")


def file_db_cache_lock(filename: str, timeout=10):
    return cache_lock(f"file_dict_{filename}_lock", timeout)


class FileDict:
    def __init__(self, filepath: str):
        self.filepath = filepath

    def __str__(self):
        if not os.path.isfile(self.filepath):
            return "{}"

        try:
            with open(self.filepath, "r", encoding="utf-8") as f:
                return f.read()
        except IOError as e:
            logger.warning(f"Cannot read {self.filepath}. Original error: {e}")

    def get(self, key: str, default: None):
        current_dict = self.data_dict()

        return current_dict.get(key, default)

    def set(self, key: str, value):
        current_dict = self.data_dict()

        current_dict[key] = value

        self._write_on_file(json.dumps(current_dict))

    def remove(self, key: str):
        current_dict = self.data_dict()

        current_dict.pop(key)

        self._write_on_file(json.dumps(current_dict))

    def reset(self, text=""):
        self._write_on_file(text)

    def data_dict(self) -> dict:
        return json.loads(str(self))

    def _write_on_file(self, data: str, write_flag="w"):
        file_utils.ensure_path_exists(os.path.dirname(self.filepath))

        try:
            with file_db_cache_lock(self.filepath):
                with open(self.filepath, write_flag, encoding="utf-8") as f:
                    f.write(data)
        except IOError as e:
            logger.warning(
                f"Cannot write '{data}' on {self.filepath}. Original error: {e}"
            )
