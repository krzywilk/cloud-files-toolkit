import soundfile as sf
from google.cloud import storage
from serializers.cloud_serializers.abstract_file_serializers import LocalToRemoteSerializer


class AudioGoogleSerializer(LocalToRemoteSerializer):

    def __init__(self, audio_content, sampling_rate,ext:str, tmp_folder_path: str):
        super().__init__(tmp_folder_path, ext)
        self._audio_buffer = audio_content
        self._sampling_rate = sampling_rate

    def serialize(self, dst_bucket_name: str, dst_bucket_file_key: str):
        storage_client = storage.Client()
        bucket = storage_client.bucket(dst_bucket_name)
        tmp_file_key = dst_bucket_file_key
        if dst_bucket_file_key[-len(self.ext):] != self.ext:
            tmp_file_key = dst_bucket_file_key+'.'+self.ext
        blob = bucket.blob(tmp_file_key )
        blob.upload_from_filename(str(self._tmp_file_Path))

    def _write_to_local(self):
        sf.write(str(self._tmp_file_Path), self._audio_buffer, self._sampling_rate)


