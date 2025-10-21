import subprocess
from pathlib import Path

def compress_video(
    video_path: Path,
    output_path: Path,
    crf: int = 13,
    slice_start: str = None,
    slice_duration: str = None,
    threads: int = 5,
    codec: str = "libx264"
):
    """
    Compress a video using ffmpeg with optional slicing.

    Args:
        video_path (Path): Path to the input video.
        output_path (Path): Desired output video path.
        crf (int): Constant Rate Factor (lower is better quality). Default is 23.
        slice_start (str): Optional start time (e.g., "00:01:00").
        slice_duration (str): Optional duration (e.g., "30").
        threads (int): Number of threads to use. Default is 5.
        codec (str): Video codec to use. Default is 'libx264'.

    Raises:
        RuntimeError: If ffmpeg fails to run successfully.
    """

    ffmpeg_args = [
        "ffmpeg",
        "-threads", str(threads),
        "-i", str(video_path),
        "-c:v", codec,
        "-crf", str(crf),
        "-pix_fmt", "yuv420p"
    ]

    if slice_start:
        ffmpeg_args += ["-ss", str(slice_start)]
    if slice_duration:
        ffmpeg_args += ["-t", str(slice_duration)]

    ffmpeg_args += ["-y", str(output_path)]

    try:
        subprocess.run(
            ffmpeg_args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE,
            check=True
        )
    except subprocess.CalledProcessError as e:
        err = e.stderr.decode(errors='replace')
        error_index = err.lower().find("error")
        error_message = err[error_index:] if error_index != -1 else err
        if output_path.exists():
            output_path.unlink()
        raise RuntimeError(f"FFmpeg compression failed:\n{error_message}")
    
