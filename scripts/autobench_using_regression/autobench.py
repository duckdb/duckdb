import argparse
from datetime import datetime
import pathlib
import re
import statistics

from regression.benchmark import BenchmarkRunner, BenchmarkRunnerConfig
import pandas as pd
import plotly.graph_objects as go
from pathlib import Path
import kaleido
import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__), "regression"))

# def resolve_benchmarks(pattern: str):
#     path = pathlib.Path(args.old_path + "/" + pattern)
#
#     if path.exists():
#         # Direct file path
#         if path.is_file():
#             return [str(path)]
#         # Directory: include all *.benchmark files
#         return [str(p) for p in path.glob("*.benchmark")]
#
#     # If not an existing path, interpret as regex
#     # e.g. "benchmarks/compression/.*"
#     base = pathlib.Path(args.old_path + "/" + pattern).parent
#     regex = re.compile(pathlib.Path(pattern).name)
#     if not base.exists():
#         print(f"Error: base directory '{base}' does not exist.", file=sys.stderr)
#         sys.exit(1)
#
#     matches = [str(p) for p in base.glob("*.benchmark") if regex.fullmatch(p.name)]
#     if not matches:
#         print(f"No benchmarks matched '{pattern}'.", file=sys.stderr)
#         sys.exit(1)
#     return matches

# Set up the argument parser
parser = argparse.ArgumentParser(description="Script to run and plot benchmark comparison for old vs new builds.")

# Define the arguments
parser.add_argument("--old_path", type=str, help="Path to the old repo.", required=True)
parser.add_argument("--new_path", type=str, help="Path to the new repo.", required=True)
parser.add_argument(
    "--benchmark_pattern",
    type=str,
    required=True,
    help="Path or regex, e.g. 'benchmark/csv/read.benchmark' or 'benchmark/csv' or 'benchmark/csv/.*'"
)
# Parse the arguments
args = parser.parse_args()


# Assign parsed arguments to variables
old_runner_path = args.old_path + "/build/release/benchmark/benchmark_runner"
new_runner_path = args.new_path + "/build/release/benchmark/benchmark_runner"
benchmark_pattern = args.benchmark_pattern


if not os.path.isfile(old_runner_path):
    print(f"Failed to find old runner {old_runner_path}")
    exit(1)

if not os.path.isfile(new_runner_path):
    print(f"Failed to find new runner {new_runner_path}")
    exit(1)

# benchmark_list = resolve_benchmarks(benchmark_pattern)

config_dict = vars(args)
config_dict["root_dir"] = args.new_path # ALWAYS GET BENCHMARKS FROM NEW CHANGES

print("create runners")
old_runner = BenchmarkRunner(BenchmarkRunnerConfig.from_params(old_runner_path, args.new_path + "/" + benchmark_pattern, **config_dict))
new_runner = BenchmarkRunner(BenchmarkRunnerConfig.from_params(new_runner_path, args.new_path + "/" + benchmark_pattern, **config_dict))
print(f"\033[1;32mRunning benchmarks for {args.old_path} ...\033[0m")
old_runner.run_benchmark(benchmark_pattern, show_runner_output=True)
print(f"\033[1;32mRunning benchmarks for {args.new_path} ...\033[0m")
new_runner.run_benchmark(benchmark_pattern, show_runner_output=True)

print("\033[1;35mPlotting comparison...\033[0m")

BENCHMARK_RUNNER_REPETITIONS = 5
results = []

for i in range(len(old_runner.benchmark_list)):
    benchmark_name = old_runner.benchmark_names[i]

    old_timings = old_runner.complete_timings[i*BENCHMARK_RUNNER_REPETITIONS : (i+1)*BENCHMARK_RUNNER_REPETITIONS]
    old_median = statistics.median(old_timings)

    new_timings = new_runner.complete_timings[i*BENCHMARK_RUNNER_REPETITIONS : (i+1)*BENCHMARK_RUNNER_REPETITIONS]
    new_median = statistics.median(new_timings)

    diff = new_median - old_median
    percent_change = round((diff / old_median * 100), 2)  # normalized percent diff

    results.append({
        "benchmark": benchmark_name,
        "old_timing": old_median,
        "new_timing": new_median,
        "difference": diff,
        "percent_change": percent_change,
    })

df = pd.DataFrame(results)

# -------------------------------------------------------------------
# Plot difference
# -------------------------------------------------------------------
fig = go.Figure()

# Old timings
fig.add_trace(go.Bar(
    x=df["benchmark"],
    y=df["old_timing"],
    name="Old Timings",
    marker_color="#5285ec",
    hovertemplate="<b>%{x}</b><br>Old Median: %{y:.6f}s<br>",
))

# New timings + percentage labels
fig.add_trace(go.Bar(
    x=df["benchmark"],
    y=df["new_timing"],
    name="New Timings",
    marker_color="#d85040",
    hovertemplate="<b>%{x}</b><br>New Median: %{y:.6f}s<br>",
    text=[f"{pc:+.1f}%" for pc in df["percent_change"]],
    textposition="outside",
    textfont=dict(
        size=12,
        color=["green" if pc < 0 else "red" for pc in df["percent_change"]],
        weight="bold",
    ),
))

# Layout
fig.update_layout(
    barmode="group",
    xaxis_title="Benchmark",
    yaxis_title="Runtime (seconds)",
    height=600,
    template="plotly_white",
    xaxis=dict(
        tickangle=45,
        tickfont=dict(size=10),
        categoryorder="array",
        categoryarray=df["benchmark"],
    ),
    legend=dict(
        orientation="h",
        yanchor="bottom",
        y=1.02,
        xanchor="center",
        x=0.5,
    ),
    margin=dict(l=60, r=30, t=100, b=150),
    plot_bgcolor="rgba(245,245,245,0.9)",
    paper_bgcolor="rgba(255,255,255,1)",
)

# -------------------------------------------------------------------
# Save figure as PNG
# -------------------------------------------------------------------
output_dir = Path("../plots")
output_dir.mkdir(exist_ok=True)
current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
output_filename = output_dir / f"plot-{current_datetime}.png"

print("Generating PNG")
fig.write_image(str(output_filename), scale=2, width=1200, height=800)
print(f"Plot saved to: {output_filename}")



