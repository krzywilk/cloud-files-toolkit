import abc
import os
import uuid
from abc import ABC
from collections.abc import Generator
from pathlib import Path
import copy


class RemoteIterableFileLoader(abc.ABC):
    def __init__(self, bucket_name: str, bucket_file_key: str, tmp_folder_path: str = 'tmp/'):
        self.bucket_name = bucket_name
        self.bucket_file_key = bucket_file_key
        self.tmp_folder_path = tmp_folder_path
        if tmp_folder_path != '':
            os.makedirs(self.tmp_folder_path, exist_ok=True)
        self._tmp_file_Path = Path(tmp_folder_path, str(uuid.uuid4())).with_suffix('.' + bucket_file_key.split('.')[-1])
        self._file_buffer = None
        self._missing_ok = False

    @abc.abstractmethod
    def _fetch_file(self) -> None:
        pass

    @abc.abstractmethod
    def _read_file(self):
        """
        self._file_buffer has to be initialized. The implementation depends on file type.
        :return:
        """
        pass

    def get_tmp_file_path(self):
        return str(self._tmp_file_Path)

    @abc.abstractmethod
    def _close_reader(self):
        pass

    # with interface implementation: BEG
    def __enter__(self):
        self._fetch_file()
        self._read_file()
        return self

    def __exit__(self, exception_type, exception_value, traceback) -> None:  # TODO: exception handling
        self._tmp_file_Path.unlink(self._missing_ok)
        # self._tmp_file_Path.


class LazyRemoteIterableFileLoader(RemoteIterableFileLoader, ABC, Generator):

    def __init__(self, bucket_name: str, bucket_file_key: str, tmp_folder_path: str = 'tmp/'):
        super().__init__(bucket_name, bucket_file_key, tmp_folder_path)

    def __iter__(self):
        if self._file_buffer is None:
            raise Exception("File loading not initialized")
        while True:
            try:
                yield self.__next__()
            except (GeneratorExit, StopIteration):
                self.close()
                break

    @abc.abstractmethod
    def _get_next_item(self):
        """
        if there is no more to iterate on, return None
        :return:
        """
        pass

    def __next__(self):
        element = self._get_next_item()
        return self.send(element)

    def send(self, __value):
        if __value is None:
            raise StopIteration
        return __value

    def throw(self, __typ, __val=None, __tb=None):
        if __val is None:
            if __tb is None:
                raise __typ
        val = None
        if __tb is not None:
            val = __val.with_traceback(__tb)
        return val

    def close(self) -> None:
        try:
            self._close_reader()
            self.throw(GeneratorExit)
        except (GeneratorExit, StopIteration):
            pass
        except Exception as error:
            raise error


class EagerRemoteIterableFileLoader(RemoteIterableFileLoader, ABC):

    def __init__(self, bucket_name: str, bucket_file_key: str, tmp_folder_path: str = 'tmp/'):
        super().__init__(bucket_name, bucket_file_key, tmp_folder_path)
        self._file_buffer = None

    def get_file_buffer(self):
        return copy.deepcopy(self._file_buffer)

    def __len__(self):
        return len(self._file_buffer)

    def __getitem__(self, item):
        return self._file_buffer[item]

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._close_reader()
        super().__exit__(exc_type, exc_val, exc_tb)


