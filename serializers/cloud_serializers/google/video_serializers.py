import platform
import subprocess
import uuid
from pathlib import Path

from utils.video import write_video
import soundfile as sf
from serializers.cloud_serializers.abstract_file_serializers import LocalToRemoteSerializer
from google.cloud import storage


class VideoMP4AudioWavGoogleSerializer(LocalToRemoteSerializer):
    FFMPEG_COMMAND = 'ffmpeg -y -i {} -i {} -strict -2 -q:v 1 {}'

    def __init__(self, video_content, wav_audio_content, fps: int,sampling_rate, tmp_folder_directory: str):
        super().__init__(tmp_folder_directory, 'mp4')
        self._video_content = video_content
        self._wav_audio_content = wav_audio_content
        self.fps = fps
        self.sampling_rate = sampling_rate

    def serialize(self, dst_bucket_name: str, dst_bucket_file_key: str):
        storage_client = storage.Client()
        bucket = storage_client.bucket(dst_bucket_name)
        tmp_file_key = dst_bucket_file_key
        if dst_bucket_file_key[-len(self.ext):] != self.ext:
            tmp_file_key = dst_bucket_file_key+'.'+self.ext
        blob = bucket.blob(tmp_file_key)
        blob.upload_from_filename(str(self._tmp_file_Path))

    def _write_to_local(self):  # TODO: possible security issue, to check

        tmp_mute_path = Path(self.tmp_folder_directory, str(uuid.uuid4())).with_suffix('.mp4')
        write_video(self._video_content, str(tmp_mute_path))

        tmp_audio_path = Path(self.tmp_folder_directory, str(uuid.uuid4())).with_suffix('.wav')
        sf.write(tmp_audio_path, self._wav_audio_content, self.sampling_rate)

        command = self.FFMPEG_COMMAND.format(str(tmp_mute_path),
                                             str(tmp_audio_path),
                                             str(self._tmp_file_Path))
        subprocess.call(command, shell=platform.system() != 'Windows')

        tmp_mute_path.unlink()
        tmp_audio_path.unlink()


class VideoMP4GoogleSerializer(LocalToRemoteSerializer):
    def __init__(self, video_frames_content, fps: int, tmp_folder_directory: str):
        super().__init__(tmp_folder_directory, 'mp4')
        self._video_frames_content = video_frames_content
        self.fps = fps

    def serialize(self, dst_bucket_name: str, dst_bucket_file_key: str):
        storage_client = storage.Client()
        bucket = storage_client.bucket(dst_bucket_name)
        tmp_file_key = dst_bucket_file_key
        if dst_bucket_file_key[-len(self.ext):] != self.ext:
            tmp_file_key = dst_bucket_file_key+'.'+self.ext
        blob = bucket.blob(tmp_file_key)
        blob.upload_from_filename(str(self._tmp_file_Path))

    def _write_to_local(self):
        write_video(self._video_frames_content, str(self._tmp_file_Path))
