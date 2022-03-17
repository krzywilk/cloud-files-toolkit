import abc
import uuid
from pathlib import Path
import os


class LocalToRemoteSerializer(abc.ABC):
    def __init__(self, tmp_folder_directory: str, ext: str):
        self.tmp_folder_directory = tmp_folder_directory
        self.ext = ext.replace('.', '')
        self._tmp_file_Path = Path(self.tmp_folder_directory, str(uuid.uuid4())).with_suffix('.' + ext)
        os.makedirs(self.tmp_folder_directory, exist_ok=True)
        self._local_serializer = None

    def get_tmp_file_path(self):
        return str(self._tmp_file_Path)

    @abc.abstractmethod
    def _write_to_local(self):
        pass

    @abc.abstractmethod
    def serialize(self, dst_bucket_name: str, dst_file_path: str) -> None:  # todo: exception handle'
        pass

    def __enter__(self):
        self._write_to_local()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):  # TODO: exception handling
        self._tmp_file_Path.unlink()
