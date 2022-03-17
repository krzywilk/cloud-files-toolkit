import cv2
import numpy as np


def put_chunks_on_video(video_buffer, frames_chunks, frames_chunks_coords):
    output = []
    for p, f, c in zip(frames_chunks, video_buffer, frames_chunks_coords):
        frame = f.copy()
        y1, y2, x1, x2 = c
        p = cv2.resize(p.astype(np.uint8), (x2 - x1, y2 - y1))

        frame[y1:y2, x1:x2] = p
        output.append(frame)
    return output


def load_video_from_file(path):
    result = []
    cap = cv2.VideoCapture(path)
    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            break
        else:
            result.append(frame)
    fps = cap.get(cv2.CAP_PROP_FPS)
    cap.release()
    return result, fps


def write_video(video_frame, file_path, fourcc=cv2.VideoWriter_fourcc(*'MP4V'), fps=25):
    frame_width, frame_height = video_frame[0].shape[:2]
    out = cv2.VideoWriter(file_path, fourcc, fps,
                          (frame_height, frame_width))
    for frame in video_frame:
        out.write(frame)
    out.release()
