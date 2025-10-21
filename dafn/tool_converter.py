import pandas as pd

def fiber2events(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["t"] = df["TimeStamp"]/1000
    if not df["t"].is_monotonic_increasing:
        raise Exception("Data should already be sorted")
    df = df.reset_index()

    res = []
    for n, g in df.groupby("Name"):
        starts_arr = g.loc[g["State"] == 0, "t"].to_numpy()
        ends_arr = g.loc[g["State"] == 1, "t"].to_numpy()
        start_indices = g.loc[g["State"] == 0, "index"].to_numpy()

        if starts_arr.size > 0 and ends_arr.size > 0:
            if starts_arr[0] > ends_arr[0]:
                starts_arr = np.insert(starts_arr, 0, np.nan)
                start_indices = np.insert(start_indices, 0, -1)
            if starts_arr[-1] > ends_arr[-1]:
                ends_arr = np.append(ends_arr, np.nan)

        if starts_arr.shape != ends_arr.shape:
            raise Exception("Not same number of rise and fall events")
        durations = ends_arr - starts_arr
        if (durations < 0 ).any():
            raise Exception("Problem aligning rise and falls events")
        result = pd.DataFrame()
        result["start"] = starts_arr
        result["duration"] = durations
        result["event_name"] = n
        result["index"] = start_indices
        res.append(result)

    final_df = pd.concat(res).sort_values("index")[["event_name", "start", "duration"]].reset_index(drop=True)
    return final_df