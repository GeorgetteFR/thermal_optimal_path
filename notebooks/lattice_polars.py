import polars as pl
import math
from thermal_optimal_path.error_models import error

def iter_lattice(n, exclude_boundary=True):
    start_time = 1 if exclude_boundary else 0
    for t in range(start_time, 2 * n):
        offset = 1 if exclude_boundary else 0
        start = max(t - 2 * n + 1, -t + offset)
        end = min(2 * n - t - 1, t - offset)
        if (start + t) % 2:
            start += 1
        for x in range(start, end + 1, 2):
            t_a = (t - x) // 2
            t_b = (t + x) // 2
            yield x, t, t_a, t_b


def partition_function(series_a, series_b, temperature, error_func=None):
    if not error_func:
        error_func = error

    n = len(series_a)  

    g = [[float("nan")] * n for _ in range(n)]

    for i in range(n):
        g[0][i] = 0.0  
        g[i][0] = 0.0  
    g[0][0] = 1.0
    if n > 1:
        g[0][1] = 1.0
        g[1][0] = 1.0

    for x, t, t_a, t_b in iter_lattice(n):
        if t_b > 0:
            g_sum = g[t_a][t_b - 1]
        else:
            g_sum = 0.0
        if t_a > 0:
            g_sum += g[t_a - 1][t_b]
        if t_a > 0 and t_b > 0:
            g_sum += g[t_a - 1][t_b - 1]

        if t_a < n and t_b < n:
            g[t_a][t_b] = g_sum * math.exp(-error_func(series_a[t_a], series_b[t_b]) / temperature)

    g_df = pl.DataFrame({f"col_{i}": [row[i] for row in g] for i in range(n)})

    return g_df

def average_path(partition_function_df):
    n = partition_function_df.height

    t_values = list(range(2 * n - 1))
    total_df = pl.DataFrame({"t": t_values, "total": [0.0] * (2 * n - 1)})
    avg_df = pl.DataFrame({"t": t_values, "avg": [0.0] * (2 * n - 1)})

    for x, t, t_a, t_b in iter_lattice(n, exclude_boundary=False):
        if t_a < n and t_b < n:
            value = partition_function_df[f"col_{t_b}"][t_a]

            total_df = total_df.with_columns([
                pl.when(pl.col("t") == t).then(pl.col("total") + value).otherwise(pl.col("total")).alias("total")
            ])

            avg_df = avg_df.with_columns([
                pl.when(pl.col("t") == t).then(pl.col("avg") + x * value).otherwise(pl.col("avg")).alias("avg")
            ])

    result = avg_df["avg"] / pl.when(total_df["total"] == 0).then(1.0).otherwise(total_df["total"])

    return result
