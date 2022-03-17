import io
from loaders.cloud_loaders.abstract_iterable_file_loaders import EagerRemoteIterableFileLoader
from google.cloud import storage
import soundfile as sf


class AudioGoogleLoader(EagerRemoteIterableFileLoader):
    def __init__(self, bucket_name: str, bucket_file_key: str):
        super().__init__(bucket_name, bucket_file_key, '')
        self.sampling_rate = -1
        self._missing_ok = True

    def _fetch_file(self) -> None:
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.bucket_name)
        blob = bucket.blob(self.bucket_file_key)
        self._file_buffer = blob.download_as_string()

    def _read_file(self):
        data, sample_rate = sf.read(io.BytesIO(self._file_buffer))
        self._file_buffer = data
        self.sampling_rate = sample_rate

    def _close_reader(self):
        del self._file_buffer
        self.sampling_rate = -1
