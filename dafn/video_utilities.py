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
    res =  df.stack("scorer").stack("bodyparts").stack("coords").to_xarray()
    if res.sizes["scorer"] !=1:
        raise Exception(f"Multiple scorers not supported, got {res.sizes['scorer']}")
    res = res.isel(scorer=0, drop=True)
    return res




def annotate_video2(video_path: Path, output_path: Path, pose: xr.DataArray, skeleton=None):
    import cv2
    import numpy as np
    import xarray as xr
    import matplotlib.cm as cm
    import numba

    @numba.njit
    def stamp_all_bodyparts(frame, xs, ys, ps, masks, alphas, threshold=0.8):
        """
        Stamp multiple body parts on a frame in one Numba call.

        frame: HxWx3 uint8 array
        xs, ys: bodypart coordinates for this frame, int arrays of shape (num_bodyparts,)
        ps: confidence values for bodyparts, float array of shape (num_bodyparts,)
        masks, alphas: lists of masks and alpha arrays for each body part
        """
        num_bodyparts = xs.shape[0]
        frame_height, frame_width, _ = frame.shape
        mask_size = masks[0].shape[0]

        for bp in range(num_bodyparts):
            if ps[bp] <= threshold:
                continue
            x = xs[bp]
            y = ys[bp]

            if x <= -mask_size or y <= -mask_size:
                continue

            mask = masks[bp]
            alpha = alphas[bp]

            for i in range(mask_size):
                yi = y + i
                if yi < 0 or yi >= frame_height:
                    continue
                for j in range(mask_size):
                    xi = x + j
                    if xi < 0 or xi >= frame_width:
                        continue
                    if alpha[i, j]:
                        for c in range(3):
                            frame[yi, xi, c] = mask[i, j, c]

    if skeleton is not None:
        print("Skeleton drawing is not yet implemented")

    cap = cv2.VideoCapture(str(video_path))
    fps = int(cap.get(cv2.CAP_PROP_FPS))
    frame_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    frame_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fourcc = cv2.VideoWriter_fourcc(*"XVID")
    out = cv2.VideoWriter(str(output_path), fourcc, fps, (frame_width, frame_height))

    num_bodyparts = pose.sizes["bodyparts"]
    num_frames = pose.sizes["frame_num"]

    # Colors per bodypart
    cmap = cm.get_cmap("jet", num_bodyparts)
    colors = [tuple(int(c * 255) for c in cmap(i)[:3]) for i in range(num_bodyparts)]

    mask_size = 11
    radius = mask_size // 2

    # Precreate masks
    def make_circle_mask(color):
        d = mask_size
        yy, xx = np.ogrid[:d, :d]
        circle = (xx - radius) ** 2 + (yy - radius) ** 2 <= radius * radius

        mask = np.zeros((d, d, 3), dtype=np.uint8)
        mask[circle] = color

        alpha = circle  # bool array, faster to use directly

        return mask, alpha

    circles = [make_circle_mask(color) for color in colors]

    # Body part coordinates
    x = (
        pose.sel(coords="x")
        .transpose("frame_num", "bodyparts")
        .fillna(-mask_size-1)
        .to_numpy()
        - mask_size / 2
    ).astype(int)

    y = (
        pose.sel(coords="y")
        .transpose("frame_num", "bodyparts")
        .fillna(-mask_size-1)
        .to_numpy()
        - mask_size / 2
    ).astype(int)

    p = pose.sel(coords="p").transpose("frame_num", "bodyparts").to_numpy()



    # Main loop
    for i in range(num_frames):
        ret, frame = cap.read()
        if not ret:
            break

        # Stamp all bodyparts at once
        stamp_all_bodyparts(frame, x[i], y[i], p[i], [c[0] for c in circles], [c[1] for c in circles])

    out.write(frame)

    cap.release()
    out.release()

def annotate_video(video_path: Path, output_path: Path, pose: xr.DataArray, radius=5):
    import cv2
    import numpy as np
    import xarray as xr
    import matplotlib.cm as cm
    import numba

    cap = cv2.VideoCapture(str(video_path))
    fps = int(cap.get(cv2.CAP_PROP_FPS))
    frame_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    frame_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fourcc = cv2.VideoWriter_fourcc(*"XVID")
    out = cv2.VideoWriter(str(output_path), fourcc, fps, (frame_width, frame_height))

    num_bodyparts = pose.sizes["bodyparts"]
    num_frames = pose.sizes["frame_num"]

    # Colors per bodypart
    cmap = cm.get_cmap("jet", num_bodyparts)
    colors = np.array([tuple(int(c * 255) for c in cmap(i)[:3]) for i in range(num_bodyparts)])

    # Precompute circle offsets
    def circle_offsets(radius):
        yy, xx = np.ogrid[-radius:radius+1, -radius:radius+1]
        circle = xx**2 + yy**2 <= radius**2
        return np.column_stack((xx[circle], yy[circle]))

    circle_coords = [circle_offsets(radius) for _ in range(num_bodyparts)]

    # Body part coordinates
    x = (
        pose.sel(coords="x")
        .transpose("frame_num", "bodyparts")
        .fillna(-radius-1)
        .to_numpy()
        .astype(int)
    )

    y = (
        pose.sel(coords="y")
        .transpose("frame_num", "bodyparts")
        .fillna(-radius-1)
        .to_numpy()
        .astype(int)
    )

    p = pose.sel(coords="p").transpose("frame_num", "bodyparts").to_numpy()

    @numba.njit
    def stamp_circles(frame, xs, ys, ps, coords_list, colors, threshold=0.8):
        num_bodyparts = xs.shape[0]
        frame_h, frame_w, _ = frame.shape

        for bp in range(num_bodyparts):
            if ps[bp] <= threshold:
                continue

            cx = xs[bp]
            cy = ys[bp]

            if cx <= -radius or cy <= -radius:
                continue

            coords = coords_list[bp]
            color = colors[bp]

            for k in range(coords.shape[0]):
                xi = cx + coords[k, 0]
                yi = cy + coords[k, 1]

                if 0 <= xi < frame_w and 0 <= yi < frame_h:
                    for c in range(3):
                        frame[yi, xi, c] = color[c]

    # Main loop
    for i in range(num_frames):
        ret, frame = cap.read()
        if not ret:
            break

        stamp_circles(frame, x[i], y[i], p[i], circle_coords, colors)

        out.write(frame)

    cap.release()
    out.release()


            # cv2.circle(frame, (int(), int(y[frame_num, bodypart_num])), 5, colors[bodypart_num], -1)

        # frame_data = pose.sel(frame_num=frame_num).to_dataset()
        # coords = all_coords[frame_idx]
        # points = np.array(coords).reshape(num_bodyparts, 3)[:, :2] 
        # confidence = np.array(coords).reshape(num_bodyparts, 3)[:, 2]  

        # for i, j in skeleton_indices:
        #     if (
        #         not np.isnan(points[i]).any() and not np.isnan(points[j]).any()
        #         and confidence[i] > 0.8 and confidence[j] > 0.8
        #     ):
        #         cv2.line(frame, tuple(points[i].astype(int)), tuple(points[j].astype(int)), (0, 0, 0), 2)


        # for idx, (x, y) in enumerate(points):
        #     if not np.isnan(x) and not np.isnan(y) and confidence[idx] > 0.8:
        #         bodypart_name = bodyparts[idx]
        #         color = bodypart_colors[bodypart_name]                
        #         bgr_color = (color[2], color[1], color[0])         
        #         cv2.circle(frame, (int(x), int(y)), 5, bgr_color, -1)
        # out.write(frame)

        # frame_idx += 1
        # if frame_idx >= len(all_coords):  
        #     break

        