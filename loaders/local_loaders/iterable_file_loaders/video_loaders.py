from loaders.local_loaders.abstract_iterable_file_loaders import LazyIterableFileLoader, EagerIterableFileLoader
import cv2


class LazyVideoReader(LazyIterableFileLoader):

    def __init__(self, file_path):
        super().__init__(file_path)
        self.frame_num = -1

    def _get_next_item(self):
        frame = None
        if self._file_buffer.isOpened():
            ret, frame = self._file_buffer.read()
        self.frame_num += 1
        return frame

    def _read_file(self):
        self._file_buffer = cv2.VideoCapture(self._file_path)

    def _close_file(self):
        self._file_buffer.release()


class EagerVideoReader(EagerIterableFileLoader):
    def _read_file(self):
        self._file_buffer = []
        file_reader = cv2.VideoCapture(self._file_path)
        read, frame = file_reader.read()
        while read:
            self._file_buffer.append(frame)
            read, frame = file_reader.read()

    def _close_file(self):
        del self._file_buffer
