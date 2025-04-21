import pandas as pd
import asyncio
import re
import csv
from googletrans import Translator

# Create a translator instance
translator = Translator()

# Translation function that preserves placeholders properly
async def translate_text_with_placeholders(text):
    if not isinstance(text, str):
        return text
    
    # Extract all placeholder tags
    placeholder_pattern = re.compile(r'<([^>]+)>')
    placeholders = placeholder_pattern.findall(text)
    
    # Replace placeholders with special tokens that won't be translated
    temp_text = text
    for i, placeholder in enumerate(placeholders):
        full_tag = f'<{placeholder}>'
        # Use a token that won't be modified by translation
        temp_text = temp_text.replace(full_tag, f'__PH{i}__')
    
    # Translate the text with placeholders removed
    try:
        translation = await translator.translate(temp_text, src='en', dest='vi')
        translated_text = translation.text
        
        # Restore original placeholders
        for i, placeholder in enumerate(placeholders):
            translated_text = translated_text.replace(f'__PH{i}__', f'<{placeholder}>')
        
        return translated_text
    except Exception as e:
        print(f"Translation error: {e}")
        return text  # Return original if translation fails

async def process_csv_file(input_file, output_file, columns_to_translate):
    print(f"Processing {input_file}...")
    
    # First read the original file to preserve exact format
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        original_rows = list(reader)
        headers = original_rows[0]
    
    # Create a DataFrame for easier processing
    df = pd.read_csv(input_file, keep_default_na=False)
    
    # Add new columns for Vietnamese translations
    for column in columns_to_translate:
        if column not in df.columns:
            print(f"Column {column} not found in {input_file}")
            continue
        
        print(f"Translating column '{column}'...")
        
        # Create a new column name for Vietnamese
        vi_column = f"{column}_vi"
        
        # Translate each cell
        translation_tasks = [translate_text_with_placeholders(text) for text in df[column]]
        df[vi_column] = await asyncio.gather(*translation_tasks)
    
    # Prepare the output rows, preserving original format and adding new columns
    output_rows = [headers + [f"{col}_vi" for col in columns_to_translate]]
    
    # Get the translated values into the same format as original
    for i, row in enumerate(original_rows[1:], 1):
        new_row = row.copy()
        
        # Add the translated columns
        for column in columns_to_translate:
            if column in df.columns:
                col_idx = headers.index(column)
                vi_column = f"{column}_vi"
                translated_value = df.loc[i-1, vi_column]
                
                # Preserve quotes if original value had them
                if (len(row) > col_idx and 
                    isinstance(row[col_idx], str) and 
                    row[col_idx].startswith('"') and 
                    row[col_idx].endswith('"')):
                    if not (translated_value.startswith('"') and translated_value.endswith('"')):
                        translated_value = f'"{translated_value}"'
                
                new_row.append(translated_value)
        
        output_rows.append(new_row)
    
    # Write the final CSV with identical formatting
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        writer.writerows(output_rows)
    
    print(f"Successfully translated and saved to {output_file}")

async def main():
    # Define files and columns to translate
    files_to_process = [
        {
            "input": "piles/pile_of_device_names.csv", 
            "output": "vietnamese_piles/vietnamese_pile_of_device_names.csv",
            "columns": ["description"]
        },
        {
            "input": "piles/pile_of_device_actions.csv", 
            "output": "vietnamese_piles/vietnamese_pile_of_device_actions.csv",
            "columns": ["english_phrase"]
        },
        {
            "input": "piles/pile_of_templated_actions.csv", 
            "output": "vietnamese_piles/vietnamese_pile_of_templated_actions.csv",
            "columns": ["english_phrase"]
        },
        {
            "input": "piles/pile_of_durations.csv", 
            "output": "vietnamese_piles/vietnamese_pile_of_durations.csv",
            "columns": ["english_name"]            
        },
        {
            "input": "piles/pile_of_status_requests.csv", 
            "output": "vietnamese_piles/vietnamese_pile_of_status_requests.csv",
            "columns": ["english_phrase", "assistant_response"]                
        },
        {
            "input": "piles/pile_of_specific_actions.csv", 
            "output": "vietnamese_piles/vietnamese_pile_of_specific_actions.csv",
            "columns": ["english_phrase"]                
        },
        {
            "input": "piles/pile_of_responses.csv", 
            "output": "vietnamese_piles/vietnamese_pile_of_responses.csv",
            "columns": ["response"]  
        }
    ]

    # Process all files
    for file_info in files_to_process:
        await process_csv_file(
            file_info["input"], 
            file_info["output"], 
            file_info["columns"]
        )

# Run the translation process
if __name__ == "__main__":
    asyncio.run(main())