import pandas as pd
import asyncio
import re
from googletrans import Translator

# Helper function to identify placeholder tags
def find_placeholders(text):
    if not isinstance(text, str):
        return []
    return re.findall(r'<([^>]+)>', text)

# Create a translator instance
translator = Translator()

# Translation function with placeholder preservation
async def translate_with_placeholders(text):
    if not isinstance(text, str):
        return text
        
    # Find all placeholders
    placeholders = find_placeholders(text)
    placeholder_map = {}
    
    # Replace placeholders with unique markers
    modified_text = text
    for i, placeholder in enumerate(placeholders):
        marker = f"PLACEHOLDER_{i}"
        placeholder_tag = f"<{placeholder}>"
        placeholder_map[marker] = placeholder_tag
        modified_text = modified_text.replace(placeholder_tag, marker)
    
    # Translate the modified text
    try:
        # Handle quoted strings by stripping quotes first
        clean_text = modified_text.strip('"') if modified_text.startswith('"') and modified_text.endswith('"') else modified_text
        
        translation = await translator.translate(clean_text, src='en', dest='vi')
        translated = translation.text
        
        # Restore placeholders
        for marker, placeholder_tag in placeholder_map.items():
            translated = translated.replace(marker, placeholder_tag)
        
        return translated
    except Exception as e:
        print(f"Translation error for '{text}': {e}")
        return text  # Return original text if translation fails

async def process_file(filename, output_filename, columns_to_translate):
    # Read the CSV file
    try:
        df = pd.read_csv(filename)
    except Exception as e:
        print(f"Error reading file {filename}: {e}")
        return
    
    # Process each column that needs translation
    for column in columns_to_translate:
        if column not in df.columns:
            print(f"Column {column} not found in {filename}")
            continue
            
        print(f"Translating column {column} in {filename}...")
        
        # Create tasks for all translations
        tasks = [translate_with_placeholders(text) for text in df[column]]
        
        # Execute all translations concurrently
        translated_texts = await asyncio.gather(*tasks)
        
        # Add as a new column with '_vi' suffix
        df[f"{column}_vi"] = translated_texts
    
    # Save with UTF-8 encoding
    df.to_csv(output_filename, index=False, encoding='utf-8')
    print(f"Successfully translated and saved to {output_filename} with UTF-8 encoding")

async def main():
    # Define files and columns to translate
    files_to_process = [
        {
            "input": "piles/pipe_of_device_names.csv", 
            "output": "vietnamese_piles/vietnamese_pipe_of_device_names.csv",
            "columns": ["description"]
        },
        {
            "input": "piles/pipe_of_device_actions.csv", 
            "output": "vietnamese_piles/vietnamese_pipe_of_device_actions.csv",
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
        await process_file(file_info["input"], file_info["output"], file_info["columns"])

# Run the translation process
if __name__ == "__main__":
    asyncio.run(main())