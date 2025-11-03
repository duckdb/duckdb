import pandas as pd
import plotly.graph_objects as go
from pathlib import Path
from datetime import datetime
import argparse

import kaleido
# kaleido.get_chrome_sync()

# Set up the argument parser
parser = argparse.ArgumentParser(description="Script called by autobench.sh toplot benchmark comparisons")

parser.add_argument("--new_path", type=str, help="Path to the new repo.", required=True)
args = parser.parse_args()
new_repo_path = args.new_path

# -------------------------------------------------------------------
# File loading
# -------------------------------------------------------------------
old_file_path = Path(f"{new_repo_path}/plots/timings/timings_old.out")
new_file_path = Path(f"{new_repo_path}/plots/timings/timings_new.out")

if not old_file_path.exists() or not new_file_path.exists():
    raise FileNotFoundError("Both 'timings_old.out' and 'timings_new.out' must exist in the current directory.")

def parse_benchmark_file(filepath):
    df = pd.read_csv(filepath, sep=r'\s+', header=0)
    return df

old_df = parse_benchmark_file(old_file_path)
new_df = parse_benchmark_file(new_file_path)

if len(old_df) != len(new_df):
    raise ValueError(f"File length mismatch! Old: {len(old_df)} rows, New: {len(new_df)} rows")

# -------------------------------------------------------------------
# Add short_name to raw dataframes (for scatter plot)
# -------------------------------------------------------------------
old_df['short_name'] = old_df['name'].apply(lambda x: x.split('/')[-1].replace('.benchmark', ''))
new_df['short_name'] = new_df['name'].apply(lambda x: x.split('/')[-1].replace('.benchmark', ''))


# -------------------------------------------------------------------
# Data aggregation (for median bars)
# -------------------------------------------------------------------
old_median = old_df.groupby('name')['timing'].median().reset_index()
old_median.columns = ['name', 'old_timing']

new_median = new_df.groupby('name')['timing'].median().reset_index()
new_median.columns = ['name', 'new_timing']

comparison = pd.merge(old_median, new_median, on='name')
comparison['difference'] = comparison['new_timing'] - comparison['old_timing']
comparison['percent_change'] = (comparison['difference'] / comparison['old_timing'] * 100).round(2)
comparison['short_name'] = comparison['name'].apply(lambda x: x.split('/')[-1].replace('.benchmark', ''))
comparison = comparison.sort_values('short_name').reset_index(drop=True)

# -------------------------------------------------------------------
# Plotly figure (using reference logic)
# -------------------------------------------------------------------
fig = go.Figure()

# --- Group 1: Old Timings ---

# Bar for 'Old' Median
fig.add_trace(go.Bar(
    x=comparison['short_name'],
    y=comparison['old_timing'],
    name='Old Timings',
    marker=dict(color='#5285ec', line=dict(width=0)),
    customdata=comparison[['name', 'percent_change']],
    hovertemplate='<b>%{customdata[0]}</b><br>'
                  'Old Median: %{y:.6f}s<br>',
    width=0.4,
    offsetgroup='old',
    legendgroup='old',
))

# # Scatter for all 'Old' points
# fig.add_trace(go.Scatter(
#     x=old_df['short_name'],
#     y=old_df['timing'],
#     mode='markers',
#     name='All Points',
#     marker=dict(color="LightSteelBlue", size=4),
#     offsetgroup='old',
#     legendgroup='old',
#     hovertemplate='<b>%{x}</b><br>Old Run: %{y:.6f}s<extra></extra>',
#     visible=False  # Initially hidden
# ))

# --- Group 2: New Timings ---

# Bar for 'New' Median with percentage change labels
fig.add_trace(go.Bar(
    x=comparison['short_name'],
    y=comparison['new_timing'],
    name='New Timings',
    marker=dict(color='#d85040', line=dict(width=0)),
    customdata=comparison[['name', 'percent_change']],
    hovertemplate='<b>%{customdata[0]}</b><br>'
                  'New Median: %{y:.6f}s<br>',
    width=0.4,
    offsetgroup='new',
    legendgroup='new',
    # Add percentage change labels
    text=[f"{pc:+.1f}%" for pc in comparison['percent_change']],
    textposition='outside',
    textfont=dict(
        weight="bold",
        size=12,
        color=['green' if pc < 0 else 'red' for pc in comparison['percent_change']]
    )
))

# # Scatter for all 'New' points
# fig.add_trace(go.Scatter(
#     x=new_df['short_name'],
#     y=new_df['timing'],
#     mode='markers',
#     name='All Points',
#     marker=dict(color="LightSteelBlue", size=4),
#     offsetgroup='new',
#     legendgroup='new',
#     hovertemplate='<b>%{x}</b><br>New Run: %{y:.6f}s<extra></extra>',
#     visible=False  # Initially hidden
# ))


# --- Layout with Toggle Button ---

fig.update_layout(
    barmode='group',
    scattermode='group',
    xaxis_title="Benchmark",
    yaxis_title="Runtime (seconds)",
    height=600,
    template='plotly_white',
    xaxis=dict(
        categoryorder='array',
        categoryarray=comparison['short_name'],
        tickangle=45,
        tickfont=dict(size=10)
    ),
    legend=dict(
        orientation="h",
        yanchor="bottom",
        y=1.02,
        xanchor="center",
        x=0.5,
        groupclick='toggleitem'
    ),
    margin=dict(l=60, r=30, t=100, b=150),
    plot_bgcolor='rgba(245,245,245,0.9)',
    paper_bgcolor='rgba(255,255,255,1)',
    shapes=[
        dict(
            type="rect",
            xref="paper", yref="paper",
            x0=0, y0=0, x1=1, y1=1,
            line=dict(color="rgba(0,0,0,0)", width=0),
            fillcolor="white",
            layer="below"
        )
    ],
    # # Add toggle button for scatter points
    # updatemenus=[
    #     dict(
    #         type="buttons",
    #         direction="left",
    #         buttons=[
    #             dict(
    #                 args=[{"visible": [True, False, True, False]}],
    #                 label="Show Medians Only",
    #                 method="restyle"
    #             ),
    #             dict(
    #                 args=[{"visible": [True, True, True, True]}],
    #                 label="Show All Timings",
    #                 method="restyle"
    #             )
    #         ],
    #         pad={"r": 10, "t": 10},
    #         showactive=True,
    #         x=0.0,
    #         xanchor="left",
    #         y=1.15,
    #         yanchor="top"
    #     )
    # ]
)
# -------------------------------------------------------------------
# Save to PNG file with current datetime
# -------------------------------------------------------------------
current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
output_filename = f"{new_repo_path}/plots/plot-{current_datetime}.png"

# Requires the 'kaleido' package for static image export
fig.write_image(output_filename, scale=2, width=1200, height=800)
print(f"Plot saved to: {output_filename}")
fig.show()