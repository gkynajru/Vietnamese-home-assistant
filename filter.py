import pandas as pd
import os
import re
import csv
from pathlib import Path

def load_filter_configuration(filter_config_path):
    """
    Load the filter configuration from CSV file.
    Returns a dictionary of {device_type.device_name: should_be_removed}
    """
    filter_df = pd.read_csv(filter_config_path)
    filter_dict = {}
    
    for _, row in filter_df.iterrows():
        device_type = row['device_type']
        device_name = row['device_name_or_id']
        percentage_removed = row['percentage_removed']
        
        # Create a key in format device_type.device_name
        key = f"{device_type}.{device_name}"
        # 100% means remove, 0% means keep
        filter_dict[key] = (percentage_removed == 100)
    
    return filter_dict

def should_remove_row(row, filter_dict, device_id_column):
    """
    Check if a row should be removed based on the filter configuration.
    """
    if device_id_column not in row:
        return False
    
    device_id = row[device_id_column]
    
    # Handle case where device_id might not be a string
    if not isinstance(device_id, str):
        return False
    
    # Check if this device should be removed
    for device_pattern, should_remove in filter_dict.items():
        if should_remove and device_pattern in device_id:
            return True
    
    return False

def filter_csv_file(input_path, output_path, filter_dict, device_id_column):
    """
    Filter a CSV file based on the filter configuration.
    """
    try:
        df = pd.read_csv(input_path, encoding='utf-8')
        
        # Skip if the device_id_column is not in this file
        if device_id_column not in df.columns:
            print(f"Column {device_id_column} not found in {input_path}, skipping...")
            df.to_csv(output_path, index=False, encoding='utf-8')
            return 0
        
        original_count = len(df)
        
        # Create a mask for rows to keep
        rows_to_keep = []
        for _, row in df.iterrows():
            remove = should_remove_row(row, filter_dict, device_id_column)
            rows_to_keep.append(not remove)
        
        # Apply the filter
        filtered_df = df[rows_to_keep]
        filtered_count = len(filtered_df)
        
        # Save the filtered data
        filtered_df.to_csv(output_path, index=False, encoding='utf-8', quoting=csv.QUOTE_MINIMAL)
        
        removed_count = original_count - filtered_count
        print(f"Filtered {input_path}: Removed {removed_count} rows out of {original_count}")
        return removed_count
    
    except Exception as e:
        print(f"Error filtering {input_path}: {str(e)}")
        return 0

def main():
    # Configuration
    filter_config_path = "device_filter_config.csv"
    piles_directory = "piles"
    output_directory = "filtered_piles"
    
    # Create output directory if it doesn't exist
    os.makedirs(output_directory, exist_ok=True)
    
    # Load filter configuration
    filter_dict = load_filter_configuration(filter_config_path)
    print(f"Loaded filter configuration with {sum(filter_dict.values())} devices to remove")
    
    # List of files to process with their device ID column
    files_to_process = [
        {"file": "pile_of_device_names.csv", "device_id_column": "device_name"},
        {"file": "pile_of_device_actions.csv", "device_id_column": "device_name"},
        {"file": "pile_of_specific_actions.csv", "device_id_column": "device_name"},
        {"file": "pile_of_templated_actions.csv", "device_id_column": "device_type"},
        {"file": "pile_of_status_requests.csv", "device_id_column": "device_type"}
    ]
    
    # Process each file
    total_removed = 0
    for file_info in files_to_process:
        input_path = os.path.join(piles_directory, file_info["file"])
        output_path = os.path.join(output_directory, file_info["file"])
        
        if os.path.exists(input_path):
            removed = filter_csv_file(
                input_path, 
                output_path, 
                filter_dict, 
                file_info["device_id_column"]
            )
            total_removed += removed
        else:
            print(f"File not found: {input_path}")
    
    print(f"Filtering complete. Total rows removed: {total_removed}")

if __name__ == "__main__":
    main()