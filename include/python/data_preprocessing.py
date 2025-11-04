import deepl
import pandas as pd
import numpy as np
import contractions
import re
import emoji 
from math import ceil

def create_smart_batches(review_list: list):
    '''Yields batches that respect DeepL's item count and payload size limits.'''

    # DeepL API limits: 50 items and 128 KiB (131,072 bytes) per request.
    # Use a safe character limit to stay well under the byte limit.
    ITEM_LIMIT = 50
    CHAR_LIMIT = 100_000

    current_batch = []
    current_batch_chars = 0

    for review_text in review_list:
        text = str(review_text) if pd.notna(review_text) else ""
        text_len = len(text)

        # Handle a single review that is too large
        if text_len > CHAR_LIMIT:
            print(f"Warning: A single review is longer than {CHAR_LIMIT} chars and will be translated in its own batch. Length: {text_len}")
            # If the current batch isn't empty, send it first
            if current_batch:
                yield current_batch
            # Send the huge review by itself
            yield [text]
            # Reset
            current_batch = []
            current_batch_chars = 0
            continue

        # Check if adding this review would break the limits
        if (len(current_batch) + 1 > ITEM_LIMIT) or \
           (current_batch_chars + text_len > CHAR_LIMIT):
            
            # Send the current batch *before* adding the new review
            yield current_batch
            
            # Start a new batch with the current review
            current_batch = [text]
            current_batch_chars = text_len
        else:
            # Add the review to the current batch
            current_batch.append(text)
            current_batch_chars += text_len

    # After the loop, send the last remaining batch
    if current_batch:
        yield current_batch

def translate_reviews(df:pd.DataFrame, deepl_auth_key:str) -> pd.DataFrame:
    '''Translate non-english reviews in a dataframe.'''

    # Get the rows and list of text to translate
    non_english_mask = (df['lang'] != 'en') & (df['review_text'].notna())
    reviews_list = df.loc[non_english_mask, 'review_text'].tolist()
    total_reviews = len(reviews_list)

    if total_reviews == 0:
        print("No non-English reviews to translate.")
        return df
    print(f"Found {total_reviews} non-English reviews. Creating smart batches...")

    translated_texts = []
    # Convert the list into a list of batches (for progress tracking)
    all_batches = list(create_smart_batches(reviews_list))
    total_batches = len(all_batches)
    print(f"Total smart batches created: {total_batches}")

    try:
        translator = deepl.Translator(deepl_auth_key)

        for i, batch in enumerate(all_batches):
            if not batch: # Skip empty batches
                continue
                
            print(f"Translating batch {i+1} of {total_batches} (items: {len(batch)})...")
            
            results = translator.translate_text(batch, target_lang="EN-US")
            translated_texts.extend([result.text for result in results])

        print("Batch translation successful.")

        # Map the results back to the original DataFrame
        df.loc[non_english_mask, 'review_text'] = translated_texts
        return df
    
    except Exception as e:
        print(f"Batch text translation failed on batch {i+1} of {total_batches}: {e}")
        return df
    

def convert_data_types(df:pd.DataFrame) -> pd.DataFrame:
    df['travelDate'] = pd.to_datetime(df['travelDate'], format='%Y-%m' ,errors='coerce')
    df['publishedDate'] = pd.to_datetime(df['publishedDate'], errors='coerce')
    return df 

def handle_missing_values(df:pd.DataFrame) -> pd.DataFrame:
    df = df.replace({pd.NaT: None, np.nan: None})
    df['title'] = df['title'].fillna('')
    df['text'] = df['text'].fillna('')
    df['tripType'] = df['tripType'].replace('NONE', None)
    return df 

def handle_duplicates(df:pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=['text'], keep='first')
    return df

def combine_review_text(row):
    title = row['title']
    text = row['text']
    
    has_title = pd.notna(title) and str(title).strip() != ""
    has_text = pd.notna(text) and str(text).strip() != ""
    
    if has_title and has_text:
        return str(title) + ". " + str(text)
    elif has_title:
        return str(title)
    elif has_text:
        return str(text)
    else:
        return None

def remove_whitespaces(text:str) -> str:
    return re.sub(r'\s+', ' ', text).strip()

def lowercase_text(text:str) -> str:
    return text.lower()

def remove_urls(text:str) -> str:
    url_pattern = r'https?://\S+|www\.\S+'
    return re.sub(url_pattern, ' ', text)
        
def expand_contractions(text:str) -> str:
    expanded_words = []
    for word in text.split():
        expanded_words.append(contractions.fix(word)) 

    expanded_text = ' '.join(expanded_words)
    return expanded_text

def normalize_repeated_chars(text:str) -> str: 
    # define pattern that matches any alphabetic character followed by itself 3 or more times
    repeated_char_pattern = r'([a-zA-Z])\1{3,}'

    # replace the match with the first character from the capturing group
    return re.sub(repeated_char_pattern, r'\1', text)

def demojize_text(text:str) -> str:
    '''checks if a text contains emoji or emoji meta-characters and converts them to text'''
    meta_char_pattern = r'[\u200b-\u200d\ufe0f]'
    text = re.sub(meta_char_pattern, '', text)
    if any(char in emoji.EMOJI_DATA for char in text):
        text = emoji.demojize(text, language='en')
    return text
    
def remove_noisy_chars(text:str) -> str:
    noise_char_regex = r"""[^a-zA-Z0-9\s.,?!'"-/$€%&:`’;()“”]+"""
    return re.sub(noise_char_regex, '', text)

def preprocess_data(df:pd.DataFrame, deepl_auth_key:str) -> pd.DataFrame:
    df = convert_data_types(df) 
    df = handle_missing_values(df)
    df = handle_duplicates(df)
    
    # Keep country from user location
    df['user_location'] = df['user_location'].str.replace(r'^(.*?), (.*)$', r'\2', regex=True)

    # Merge review title and text in one column
    df['review_text'] = df.apply(combine_review_text, axis=1)

    # Drop the original 'title' and 'text' columns
    df = df.drop(columns=['title', 'text'])

    # Translate non english reviews
    df = translate_reviews(df, deepl_auth_key)
    
    df['review_text'] = df['review_text'].apply(demojize_text)
    
    df['review_text'] = df['review_text'].apply(remove_urls)
    
    df['review_text'] = df['review_text'].apply(remove_whitespaces)
    
    return df