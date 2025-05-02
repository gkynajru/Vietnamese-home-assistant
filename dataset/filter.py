import pandas as pd
import os
import re
import csv
from pathlib import Path

def load_device_filter_configuration(filter_config_path):
    """
    Load the device filter configuration from CSV file.
    Returns a dictionary of {device_type.device_name: should_be_removed}
    """
    if not os.path.exists(filter_config_path):
        print(f"Filter configuration file not found: {filter_config_path}")
        return {}
        
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

def should_remove_row_device(row, filter_dict, device_id_column):
    """
    Check if a row should be removed based on the device filter configuration.
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

def should_remove_row_keywords(row, keywords=['toggle', 'flip']):
    """
    Check if a row should be removed because it contains specified keywords.
    """
    for column, value in row.items():
        if isinstance(value, str):
            # Case insensitive search for keywords
            value_lower = value.lower()
            for keyword in keywords:
                if keyword.lower() in value_lower:
                    return True
    
    return False

def filter_csv_file(input_path, output_path, device_filter_dict=None, device_id_column=None, filter_keywords=True):
    """
    Filter a CSV file based on the device filter configuration and keywords.
    """
    try:
        df = pd.read_csv(input_path, encoding='utf-8')
        original_count = len(df)
        
        # Create a mask for rows to keep
        rows_to_keep = []
        removed_by_device = 0
        removed_by_keywords = 0
        
        for _, row in df.iterrows():
            remove_device = False
            remove_keywords = False
            
            # Check device filter if applicable
            if device_filter_dict and device_id_column and device_id_column in df.columns:
                remove_device = should_remove_row_device(row, device_filter_dict, device_id_column)
            
            # Check keyword filter if applicable
            if filter_keywords:
                remove_keywords = should_remove_row_keywords(row)
            
            # Track removal stats
            if remove_device:
                removed_by_device += 1
            if remove_keywords:
                removed_by_keywords += 1
                
            # Keep row only if it passes both filters
            rows_to_keep.append(not (remove_device or remove_keywords))
        
        # Apply the filter
        filtered_df = df[rows_to_keep]
        filtered_count = len(filtered_df)
        
        # Save the filtered data
        filtered_df.to_csv(output_path, index=False, encoding='utf-8', quoting=csv.QUOTE_MINIMAL)
        
        total_removed = original_count - filtered_count
        print(f"Filtered {input_path}:")
        print(f"  - Total rows: {original_count}")
        print(f"  - Removed by device filter: {removed_by_device}")
        print(f"  - Removed by keywords (toggle/flip): {removed_by_keywords}")
        print(f"  - Remaining rows: {filtered_count}")
        
        return total_removed
    
    except Exception as e:
        print(f"Error filtering {input_path}: {str(e)}")
        return 0

def filter_all_csv_files(piles_directory, output_directory, device_filter_dict=None, filter_keywords=True):
    """
    Process all CSV files in the directory, applying both device and keyword filters.
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_directory, exist_ok=True)
    
    # List of files to process with their device ID column
    files_to_process = [
        {"file": "vietnamese_pile_of_device_names.csv", "device_id_column": "device_name"},
        {"file": "vietnamese_pile_of_device_actions.csv", "device_id_column": "device_name"},
        {"file": "vietnamese_pile_of_specific_actions.csv", "device_id_column": "device_name"},
        {"file": "vietnamese_pile_of_templated_actions.csv", "device_id_column": "device_type"},
        {"file": "vietnamese_pile_of_status_requests.csv", "device_id_column": "device_type"},
        {"file": "vietnamese_pile_of_responses.csv", "device_id_column": None},
        {"file": "vietnamese_pile_of_system_prompts.csv", "device_id_column": None},
        {"file": "vietnamese_pile_of_durations.csv", "device_id_column": None},
        {"file": "vietnamese_pile_of_media_names.csv", "device_id_column": None}
    ]
    
    # Process each listed file if it exists
    total_removed = 0
    for file_info in files_to_process:
        input_path = os.path.join(piles_directory, file_info["file"])
        output_path = os.path.join(output_directory, file_info["file"])
        
        if os.path.exists(input_path):
            removed = filter_csv_file(
                input_path, 
                output_path, 
                device_filter_dict, 
                file_info["device_id_column"],
                filter_keywords
            )
            total_removed += removed
        else:
            print(f"File not found: {input_path}")
    
    # Find and process any additional CSV files not explicitly listed
    for filename in os.listdir(piles_directory):
        if filename.endswith('.csv') and not any(info["file"] == filename for info in files_to_process):
            input_path = os.path.join(piles_directory, filename)
            output_path = os.path.join(output_directory, filename)
            
            print(f"Processing additional file: {filename}")
            removed = filter_csv_file(
                input_path, 
                output_path,
                device_filter_dict=None,  # No device filtering for unknown files
                device_id_column=None,
                filter_keywords=filter_keywords
            )
            total_removed += removed
    
    return total_removed

def main():
    # Configuration
    device_filter_config_path = "device_filtering_comparision.csv"
    piles_directory = "vietnamese_piles"
    output_directory = "filtered_piles"
    filter_keywords = True  # Set to True to filter out "toggle" and "flip"
    
    # Load device filter configuration if available
    device_filter_dict = load_device_filter_configuration(device_filter_config_path)
    if device_filter_dict:
        print(f"Loaded device filter configuration with {sum(device_filter_dict.values())} devices to remove")
    else:
        print("No device filter configuration loaded. Only keyword filtering will be applied.")
    
    # Process all CSV files
    total_removed = filter_all_csv_files(
        piles_directory, 
        output_directory, 
        device_filter_dict, 
        filter_keywords
    )
    
    print(f"\nFiltering complete. Total rows removed: {total_removed}")
    print(f"Filtered files saved to: {output_directory}")

if __name__ == "__main__":
    main()