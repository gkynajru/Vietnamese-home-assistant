import pandas as pd
import asyncio
import re
import time
from googletrans import Translator, LANGUAGES

# Helper function to identify placeholder tags
def find_placeholders(text):
    if not isinstance(text, str):
        return []
    return re.findall(r'<([^>]+)>', text)

# Create a translator instance
translator = Translator()

# Translation function with placeholder preservation and retry logic
async def translate_with_placeholders(text, retry_count=3, delay=2):
    if not isinstance(text, str):
        return text
    
    if text.strip() == "":
        return text
        
    # Find all placeholders
    placeholders = find_placeholders(text)
    placeholder_map = {}
    
    # Replace placeholders with unique markers
    modified_text = text
    for i, placeholder in enumerate(placeholders):
        marker = f"__PLACEHOLDER_{i}__"
        placeholder_tag = f"<{placeholder}>"
        placeholder_map[marker] = placeholder_tag
        modified_text = modified_text.replace(placeholder_tag, marker)
    
    # Retry logic
    for attempt in range(retry_count):
        try:
            # Wait before retrying to avoid rate limits
            if attempt > 0:
                await asyncio.sleep(delay)
                
            translation = await asyncio.to_thread(
                translator.translate, 
                modified_text, 
                src='en', 
                dest='vi'
            )
            
            if not translation or not translation.text:
                continue
                
            translated = translation.text
            
            # Restore placeholders
            for marker, placeholder_tag in placeholder_map.items():
                translated = translated.replace(marker, placeholder_tag)
            
            return translated
        except Exception as e:
            print(f"Translation error (attempt {attempt+1}/{retry_count}) for '{text[:30]}...': {e}")
            if attempt == retry_count - 1:
                # On final attempt, return original
                return text

async def process_file(input_file, output_file, columns_to_translate):
    print(f"Processing file: {input_file}")
    
    try:
        # Read the CSV file with careful handling of quotes and line breaks
        df = pd.read_csv(
            input_file, 
            keep_default_na=False,
            quoting=pd.io.common.csv.QUOTE_MINIMAL,
            escapechar='\\',
            encoding='utf-8'
        )
        
        original_column_count = len(df.columns)
        original_row_count = len(df)
        print(f"Original file has {original_row_count} rows and {original_column_count} columns")
        
        # Process each column separately to avoid corrupting structure
        for column in columns_to_translate:
            if column not in df.columns:
                print(f"Column '{column}' not found in {input_file}")
                continue
                
            print(f"Translating column '{column}'...")
            
            # Translate row by row with delays to avoid API limits
            translated_column = []
            batch_size = 5  # Process in small batches
            
            for i in range(0, len(df), batch_size):
                batch = df[column].iloc[i:i+batch_size]
                
                # Create tasks for batch translations
                tasks = [translate_with_placeholders(text) for text in batch]
                
                # Execute translations and add delay between batches
                batch_translations = await asyncio.gather(*tasks)
                translated_column.extend(batch_translations)
                
                # Progress update
                print(f"Translated rows {i+1}-{min(i+batch_size, len(df))} of {len(df)}")
                
                # Delay between batches to avoid rate limits
                if i + batch_size < len(df):
                    await asyncio.sleep(2)
            
            # Add as a new column with '_vi' suffix
            df[f"{column}_vi"] = translated_column
            
        # Verify the structure is preserved
        if len(df) != original_row_count:
            print(f"WARNING: Row count changed from {original_row_count} to {len(df)}")
        
        # Save with UTF-8 encoding and explicit quoting control
        df.to_csv(
            output_file, 
            index=False, 
            encoding='utf-8', 
            quoting=pd.io.common.csv.QUOTE_MINIMAL,
            escapechar='\\'
        )
        print(f"Successfully saved to {output_file}")
        
    except Exception as e:
        print(f"Error processing file {input_file}: {e}")

async def main():
    # Define files and columns to translate
    files_to_process = [
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
        }
    ]

    # Process all files
    for file_info in files_to_process:
        await process_file(
            file_info["input"], 
            file_info["output"], 
            file_info["columns"]
        )

# Run the translation process
if __name__ == "__main__":
    asyncio.run(main())