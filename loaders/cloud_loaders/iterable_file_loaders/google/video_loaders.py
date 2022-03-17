from loaders.cloud_loaders.abstract_iterable_file_loaders import EagerRemoteIterableFileLoader, \
    LazyRemoteIterableFileLoader
from google.cloud import storage
from loaders.local_loaders.iterable_file_loaders.video_loaders import EagerVideoReader, LazyVideoReader


class EagerVideoGoogleLoader(EagerRemoteIterableFileLoader):

    def __init__(self, bucket_name: str, bucket_file_key: str, tmp_folder_path: str):
        super().__init__(bucket_name, bucket_file_key, tmp_folder_path)
        self.fps = -1

    def _fetch_file(self) -> None:
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.bucket_name)
        blob = bucket.blob(self.bucket_file_key)
        blob.download_to_filename(self._tmp_file_Path)

    def _read_file(self):
        reader = EagerVideoReader(self.get_tmp_file_path())
        reader.__enter__()
        self._file_buffer = reader

    def _close_reader(self):
        self._file_buffer.__exit__(None, None, None)


class LazyVideoGoogleLoader(LazyRemoteIterableFileLoader):
    def _get_next_item(self):
        return next(self._file_buffer)

    def _close_reader(self):
        self._file_buffer.__exit__(None, None, None)

    def _fetch_file(self) -> None:
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.bucket_name)
        blob = bucket.blob(self.bucket_file_key)
        blob.download_to_filename(self._tmp_file_Path)

    def _read_file(self):
        reader = LazyVideoReader(self.get_tmp_file_path())
        reader.__enter__()
        self._file_buffer = reader
