import pandas as pd
import matplotlib.pyplot as plt

# 1. Clean a TSV file (both with and without bloom filter files)
def clean_file(input_file, output_file):
    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        header = infile.readline()
        outfile.write(header)  # Write the header to the output file
        
        for line in infile:
            if line.startswith("benchmark"):
                # Split the line into columns
                columns = line.strip().split('\t')
                # Check if the "timing" column contains a valid number
                try:
                    float(columns[2])  # Attempt to convert the timing column to a float
                    outfile.write(line)
                except (IndexError, ValueError):
                    continue  # Skip rows with invalid or missing "timing" values

# 2. Load the cleaned and other TSV files
def load_data(file_path):
    return pd.read_csv(file_path, sep='\t')  # TSV uses tab as the separator

# 3. Calculate median execution times for each query
def calculate_medians(df, query_column, time_column):
    return df.groupby(query_column)[time_column].median()

# 4. Extract query IDs from names
def extract_query_ids(series):
    return series.index.to_series().str.extract(r'/([a-zA-Z0-9]+)\.benchmark$')[0]

# 5. Merge medians to align queries
def align_medians(medians_enabled, medians_disabled):
    # Combine both series into a DataFrame to align queries
    combined = pd.DataFrame({
        'With Bloom Filter': medians_enabled,
        'Without Bloom Filter': medians_disabled
    })
    return combined

# 6. Create the bar plot
def plot_medians(aligned_medians, save_path=None):
    # Extract query IDs from full names
    query_ids = extract_query_ids(aligned_medians)
    aligned_medians.index = query_ids  # Update index with query IDs
    
    x = range(len(query_ids))
    
    # Set the figure size before plotting
    plt.figure(figsize=(15, 8))  # Adjust the size of the plot
    
    plt.bar(x, aligned_medians['With Bloom Filter'], width=0.4, label='With Bloom Filter', align='center')
    plt.bar([xi + 0.4 for xi in x], aligned_medians['Without Bloom Filter'], width=0.4, label='Without Bloom Filter', align='center')
    
    plt.xticks([xi + 0.2 for xi in x], query_ids, rotation=45, ha='right')
    plt.ylabel('Median Query Execution Time (seconds)')
    plt.xlabel('Query ID')
    plt.title('Median Query Execution Times with and without Bloom Filters')
    plt.legend()
    plt.tight_layout()

    # Save plot to file as SVG if a path is provided
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight', format='svg')
        print(f"Plot saved to {save_path}")
    
    # Display the plot
    plt.show()

# Main script
file_with_bloom_dirty = "job_benchmark_with_bloom.tsv"
file_with_bloom_clean = "job_benchmark_with_bloom_clean.tsv"
file_without_bloom_dirty = "job_benchmark_without_bloom.tsv"
file_without_bloom_clean = "job_benchmark_without_bloom_clean.tsv"

# Clean both files
clean_file(file_with_bloom_dirty, file_with_bloom_clean)
clean_file(file_without_bloom_dirty, file_without_bloom_clean)

# Load data
df_with_bloom = load_data(file_with_bloom_clean)
df_without_bloom = load_data(file_without_bloom_clean)

# Calculate medians
medians_with_bloom = calculate_medians(df_with_bloom, query_column='name', time_column='timing')
medians_without_bloom = calculate_medians(df_without_bloom, query_column='name', time_column='timing')

# Align medians to ensure all queries are included
aligned_medians = align_medians(medians_with_bloom, medians_without_bloom)

# Save the plot as SVG and display it
plot_medians(aligned_medians, save_path='query_execution_times.svg')
