import os

# Folder containing the .bin files
folder_path = './bin_logs'

# Output file path
output_file = os.path.join(folder_path, 'combined_log.bin')

# Get all .bin files in the folder, sorted by name
bin_files = sorted([
    f for f in os.listdir(folder_path)
    if f.endswith('.bin') and f.startswith('log_')
])

# Combine files
with open(output_file, 'wb') as outfile:
    for filename in bin_files:
        file_path = os.path.join(folder_path, filename)
        with open(file_path, 'rb') as infile:
            outfile.write(infile.read())

print(f"Combined {len(bin_files)} files into {output_file}")
