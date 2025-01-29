import re
import json
import pandas as pd
import matplotlib.pyplot as plt

# Function to handle duplicate keys in JSON
def array_on_duplicate_keys(ordered_pairs):
    """Convert duplicate keys to arrays."""
    d = {}
    for k, v in ordered_pairs:
        if k in d:
            if isinstance(d[k], list):
                d[k].append(v)
            else:
                d[k] = [d[k], v]
        else:
            d[k] = v
    return d

# Preprocess JSON to clean trailing commas and invalid values
def preprocess_json_file(file_path):
    with open(file_path, 'r') as file:
        raw_data = file.read()
    # Remove trailing commas before } or ] (also accounts for line breaks or spaces)
    cleaned_data = re.sub(r',([\s\n\r])*([\}\]])', r'\2', raw_data, flags=re.MULTILINE)
    # Replace "nan" with "null"
    cleaned_data = cleaned_data.replace('nan', 'null')
    return cleaned_data

# Load JSON using json with preprocessing
def load_json_data(file_path):
    cleaned_data = preprocess_json_file(file_path)
    # Parse the JSON data with duplicate key handling
    data = json.loads(cleaned_data, object_pairs_hook=array_on_duplicate_keys)
    # Convert to pandas DataFrame
    df = pd.json_normalize(data)
    # Ensure "name" and "time" columns exist and drop rows where "time" is NaN
    df = df.dropna(subset=['name', 'time'])
    return df

# Function to remove rows where iteration equals 0
def remove_cold_runs(df):
    return df[df['iteration'] != 0]

def remove_faulty_runs(df):
    return df[df['result'] == 'correct']

# Sum array values in a column, or set to 0 if no array is present
def sum_array_values(df, column_name):
    # If the column contains lists (arrays), sum them, otherwise set to 0
    return df[column_name].apply(lambda x: sum(x) if isinstance(x, list) else 0)

# Calculate median execution times for each query and filter out queries with no valid runtime
def calculate_medians(df, query_column, time_column):
    # Group by 'name' and find the median 'time' for each group
    median_times = df.groupby(query_column)[time_column].median()
    
    # Merge the median times with the original dataframe to keep the whole row for each group
    # For each group, find the row that has the 'time' closest to the median
    def find_median_row(group, median_time):
        return group.iloc[(group[time_column] - median_time).abs().argmin()]

    # Apply the function to each group
    result_df = df.groupby(query_column).apply(
        lambda group: find_median_row(group, median_times.loc[group.name])
    )
    
    # Reset index to ensure the result is a flat DataFrame
    result_df = result_df.reset_index(drop=True)
    
    return result_df

# Extract query IDs from full names
def extract_query_ids(df):
    return df['name'].str.extract(r'/([a-zA0-9]+)\.benchmark$')[0]

# Merge medians to align queries
def align_medians(medians_enabled, medians_disabled):
    # Combine both dataframes into a DataFrame to align queries by their "name"
    combined = pd.merge(medians_enabled, medians_disabled, on="name", how="outer", suffixes=('_with_bloom', '_without_bloom'))
    return combined


def drop_columns(df):
    # Drop all unnecessary columns and keep only the relevant ones
    return df[['name', 'time', 'agg_build_times', 'agg_probe_times', 'agg_hash_times']]


# Create the stacked bar plot
def plot_stacked_medians(aligned_medians, save_path=None):
    # Extract query IDs from the 'name' column
    query_ids = extract_query_ids(aligned_medians)
    aligned_medians.index = query_ids  # Update index with query IDs

    # Make sure the aligned medians and query_ids have the same length
    aligned_medians = aligned_medians.reindex(query_ids).reset_index()

    x = range(len(query_ids))

    # Calculate relative speedup
    aligned_medians['relative_speedup'] = (
        (aligned_medians['time_with_bloom'] - aligned_medians['time_without_bloom']) / aligned_medians['time_without_bloom']
    )

    # Set the figure size and create a grid for two subplots
    fig, axes = plt.subplots(
        nrows=2, ncols=1, figsize=(12, 8), gridspec_kw={"height_ratios": [1, 1]}
    )

    # Top plot: Stacked bar plot for execution times
    ax1 = axes[0]
    rest_runtime = (
        aligned_medians['time_with_bloom']
        - aligned_medians['agg_build_times_with_bloom']
        - aligned_medians['agg_hash_times_with_bloom']
        - aligned_medians['agg_probe_times_with_bloom']
    )
    build_time = aligned_medians['agg_build_times_with_bloom']
    hash_time = aligned_medians['agg_hash_times_with_bloom']
    probe_time = aligned_medians['agg_probe_times_with_bloom']

    ax1.bar(x, rest_runtime, width=0.4, label='Rest', align='center', color='green')
    ax1.bar(x, build_time, width=0.4, bottom=rest_runtime, label='BF Build', align='center', color='red')
    ax1.bar(x, hash_time, width=0.4, bottom=rest_runtime + build_time, label='BF Hash (probe side)', align='center', color='orange')
    ax1.bar(x, probe_time, width=0.4, bottom=rest_runtime + build_time + hash_time, label='BF Probe', align='center', color='blue')

    # Handle queries without Bloom filter (just show the query execution time)
    aligned_medians_without_bloom = aligned_medians['time_without_bloom']
    ax1.bar([xi + 0.4 for xi in x], aligned_medians_without_bloom, width=0.4, label='Without Bloom Filter', align='center', color='black')

    # Customize the top plot
    ax1.set_xticks([xi + 0.2 for xi in x])
    ax1.set_xticklabels(query_ids, rotation=90, ha='center', size=8, family='monospace')
    ax1.set_ylabel('Query Execution Time [seconds]')
    ax1.set_title('Median Query Execution Times with/without Bloom-Filters on JOB')
    ax1.legend()
    ax1.grid(True, axis='y', linestyle='--', alpha=0.7)

    # Bottom plot: Relative speedup bar plot
    ax2 = axes[1]
    ax2.bar(x, aligned_medians['relative_speedup'], width=0.6, color='purple', label='Relative Execution Time Difference (vs. Without Bloom Filter)')

    # Center 0 on y-axis in second plot
    max_speedup = aligned_medians['relative_speedup'].max()
    min_speedup = aligned_medians['relative_speedup'].min()
    y_limit = max(abs(max_speedup), abs(min_speedup)) * 1.1
    ax2.set_ylim((-y_limit, y_limit))

    ax2.text(-0.5, ax2.get_ylim()[1] * 0.8, f'Avg. Exec Time Diff: {aligned_medians['relative_speedup'].mean():.2%}')

    # Customize the bottom plot
    ax2.set_xticks(x)
    ax2.set_xticklabels(query_ids, rotation=90, ha='center', size=8, family='monospace')
    ax2.set_ylabel('Relative Time Diff')
    ax2.set_xlabel('Query ID')
    ax2.set_title('Relative Execution Time Difference with/without Bloom-Filters')
    ax2.axhline(0, color='black', linestyle='--', linewidth=0.8, alpha=0.7)
    ax2.grid(True, axis='y', linestyle='--', alpha=0.7)
    ax2.legend()

    # Adjust layout and save plot if needed
    plt.tight_layout()
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight', format='svg')
        print(f"Plot saved to {save_path}")

    # Show the plot
    plt.show()



# Main script
file_with_bloom = "imdb_with_bloom.log"
file_without_bloom = "imdb_without_bloom.log"

# Load data from JSON files
df_with_bloom = load_json_data(file_with_bloom)
df_without_bloom = load_json_data(file_without_bloom)

# Remove rows where iteration = 0
df_with_bloom = remove_faulty_runs(remove_cold_runs(df_with_bloom))
df_without_bloom = remove_faulty_runs(remove_cold_runs(df_without_bloom))

print(df_with_bloom)

# Add aggregated build_time and probe_time columns for queries with Bloom filter
df_with_bloom['agg_build_times'] = sum_array_values(df_with_bloom, 'build_time')
df_with_bloom['agg_probe_times'] = sum_array_values(df_with_bloom, 'probe_time')
df_with_bloom['agg_hash_times'] = sum_array_values(df_with_bloom, 'hash_time')

# No need to calculate 'agg_build_times' or 'agg_probe_times' for queries without Bloom filter
df_without_bloom['agg_build_times'] = 0
df_without_bloom['agg_probe_times'] = 0
df_without_bloom['agg_hash_times'] = 0

# Calculate medians (only include valid queries)
medians_with_bloom = calculate_medians(df_with_bloom, query_column='name', time_column='time')
medians_without_bloom = calculate_medians(df_without_bloom, query_column='name', time_column='time')

medians_with_bloom = drop_columns(medians_with_bloom)
medians_without_bloom = drop_columns(medians_without_bloom)

aligned_medians = align_medians(medians_with_bloom, medians_without_bloom)


# Save the plot as SVG and display it
plot_stacked_medians(aligned_medians, save_path='query_execution_times.svg')
