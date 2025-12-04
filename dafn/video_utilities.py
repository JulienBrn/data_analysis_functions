import subprocess
from pathlib import Path
import plotly.express as px
import numpy as np, xarray as xr, tqdm, pandas as pd

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
    

def get_luminosity(annotation_num, video_path, fig_output_path, max_n_frames):
    from label_studio_sdk import LabelStudio
    import pandas as pd, cv2

    api_key="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoicmVmcmVzaCIsImV4cCI6ODA3MDg1MzIyOSwiaWF0IjoxNzYzNjUzMjI5LCJqdGkiOiJhYzAwZjJkNWNlMzM0M2M0YTdjODVlZTc2MjgzMTQxOSIsInVzZXJfaWQiOiIyNSJ9.hb4En4u-wECC6h_7iqpcwA0gztb0ngby6GZBTl-_qcE"
    label_studio_url = "http://10.24.12.184/labelstudio/"

    ls_client = LabelStudio(base_url=label_studio_url, api_key=api_key)
    data = ls_client.annotations.get(id=annotation_num).result
    led_info = {}
    for item in data:
        label = item["value"]["ellipselabels"][0]
        led_info[label] = {"x_per": item["value"]["x"], "y_per": item["value"]["y"], "radiusX_per": item["value"]["radiusX"], "radiusY_per": item["value"]["radiusY"]}
    leds = pd.DataFrame(led_info).T.to_xarray().rename(index="led_name")
    print(leds)

    cap = cv2.VideoCapture(video_path)
    fps = cap.get(cv2.CAP_PROP_FPS)
    w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    num_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    cap.release()
    image = xr.Dataset()
    image["y"] = xr.DataArray(np.arange(h), dims="y")
    image["x"] = xr.DataArray(np.arange(w), dims="x")
    image["mask"] = ((image["x"] - leds["x_per"]*w/100)**2/(leds["radiusX_per"]*w/100)**2 + (image["y"] - leds["y_per"]*h/100)**2/(leds["radiusY_per"]*h/100)**2) < 1
    print(image)

    if fig_output_path:
        cap = cv2.VideoCapture(video_path)
        ret, frame = cap.read()
        cap.release()
        fig = px.imshow(frame)
        image["color"] = xr.DataArray(["r", "g", "b", "a"], dims="color")
        image["mask_color"] = xr.DataArray([0, 200, 0, 0.5], dims="color")
        rgba_mask = image["mask"] * image["mask_color"]
        import plotly.graph_objects as go
        for i in range(rgba_mask.sizes["led_name"]):
            fig.add_trace(go.Image(z=rgba_mask.isel(led_name=i).transpose("y", "x", "color"), colormodel="rgba"))
        fig.write_html(fig_output_path)

    #Highly optimized code part, we convert everything to basic numpy and list, taking care of ordering
    n_leds = image.sizes["led_name"]
    mask_low_x = image["x"].where(image["mask"].any("y")).min("x").astype(int).to_numpy().tolist()
    mask_high_x = (image["x"].where(image["mask"].any("y")).max("x").astype(int).to_numpy()+1).tolist()
    mask_low_y = image["y"].where(image["mask"].any("x")).min("y").astype(int).to_numpy().tolist()
    mask_high_y = (image["y"].where(image["mask"].any("x")).max("y").astype(int).to_numpy()+1).tolist()
    cropped_masks = [image["mask"].isel(led_name=i).transpose("y", "x").to_numpy()[mask_low_y[i]:mask_high_y[i], mask_low_x[i]:mask_high_x[i]] for i in range(n_leds)]
    mask_low_x, mask_high_x, mask_low_y, mask_high_y

    cap = cv2.VideoCapture(video_path)
    luminosities = []

    if max_n_frames is None: 
        max_n_frames = num_frames
    else:
        max_n_frames = min(max_n_frames, num_frames)

    for i in tqdm.tqdm(range(max_n_frames), desc="Reading frames"):
        ret, frame = cap.read()
        if not ret:
            break
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        lum = [np.sum(np.where(cropped_masks[i], gray[mask_low_y[i]:mask_high_y[i], mask_low_x[i]:mask_high_x[i]], 0)) for i in range(n_leds)]
        luminosities.append(lum)

    cap.release()
    #End of highly optimized code part

    luminosities = xr.DataArray(luminosities, dims=["t", "led_name"], name="luminosity")
    luminosities["t"] = np.arange(luminosities.sizes["t"])/fps
    luminosities["t"].attrs["fs"] = fps
    luminosities = luminosities/image["mask"].sum(["y", "x"])
    return luminosities

def dlc_predict(model_path: Path, video_path: Path) -> xr.DataArray:
    import deeplabcut, tempfile
    with tempfile.TemporaryDirectory() as dlc_dest:
        print(dlc_dest)
        deeplabcut.analyze_videos(
            f'{model_path}/config.yaml',
            [str(video_path)],
            save_as_csv=False,
            gputouse=0,
            destfolder=dlc_dest
        )
        h5_file = next(Path(dlc_dest).glob("*.h5"), None)
        df = pd.read_hdf(h5_file)
    df.index.name="frame_num"
    return df.stack("scorer").stack("bodyparts").stack("coords").to_xarray()
        