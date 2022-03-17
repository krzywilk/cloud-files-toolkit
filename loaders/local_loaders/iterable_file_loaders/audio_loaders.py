from loaders.local_loaders.abstract_iterable_file_loaders import EagerIterableFileLoader
import soundfile as sf


class AudioReader(EagerIterableFileLoader):

    def __init__(self, file_path):
        super().__init__(file_path)
        self.sampling_rate = -1

    def _read_file(self):
        data, samplerate = sf.read(self._file_path)
        self._file_buffer = data
        self.sampling_rate = samplerate

    def _close_file(self):
        del self._file_buffer
