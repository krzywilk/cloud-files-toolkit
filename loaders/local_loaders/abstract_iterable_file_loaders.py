import abc
from abc import ABC
from collections.abc import Generator
import copy


class LocalIterableFileLoader(abc.ABC):
    def __init__(self, file_path):
        self._file_path = file_path
        self._file_buffer = None

    @abc.abstractmethod
    def _read_file(self):
        pass

    @abc.abstractmethod
    def _close_file(self):
        pass

    # with interface implementation: BEG
    def __enter__(self):
        self._read_file()
        return self

    def __exit__(self, exception_type, exception_value, traceback) -> None:
        self._close_file()


class LazyIterableFileLoader(LocalIterableFileLoader, ABC, Generator):

    def __init__(self, file_path):
        super().__init__(file_path)

    def __iter__(self):
        if self._file_buffer is None:
            raise Exception("File loading not initialized")
        while True:
            try:
                yield self.__next__()
            except (GeneratorExit, StopIteration):
                self.close()

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
        if __tb is not None:
            val = __val.with_traceback(__tb)
        return val

    def close(self) -> None:
        try:
            self._close_object()
            self.throw(GeneratorExit)
        except (GeneratorExit, StopIteration):
            pass
        except Exception as error:
            raise error


class EagerIterableFileLoader(LocalIterableFileLoader, ABC):

    def __init__(self, file_path):
        super().__init__(file_path)

    def get_file_buffer(self):
        return copy.deepcopy(self._file_buffer)

    def __len__(self):
        return len(self._file_buffer)

    def __getitem__(self, item):
        return self._file_buffer[item]
