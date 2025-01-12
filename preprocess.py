import json
from llm_helper import llm
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.exceptions import OutputParserException
import httpx
import time
from httpx import HTTPStatusError


# Function to clean the text by removing invalid characters
def clean_text(text):
    try:
        text.encode("utf-8")
    except UnicodeEncodeError:
        # Replace unencodable characters with an empty string
        return text.encode("utf-8", "ignore").decode("utf-8")
    return text






def process_posts(raw_file_path, processed_file_path=None):
    with open(raw_file_path, encoding='utf-8') as file:
        posts = json.load(file)
        enriched_posts = []
        for post in posts:
            post['text'] = clean_text(post['text'])
            metadata = extract_metadata(post['text'])
            post_with_metadata = post | metadata
            enriched_posts.append(post_with_metadata)
    
    # for epost in enriched_posts:
    #     print(epost)

    unified_tags = get_unified_tags(enriched_posts)
    for post in enriched_posts:
        current_tags = post['tags']
        # new_tags = {unified_tags[tag] for tag in current_tags}
        new_tags = {unified_tags.get(tag, tag) for tag in current_tags}
        post['tags'] = list(new_tags)

    with open(processed_file_path, encoding='utf-8', mode="w") as outfile:
        json.dump(enriched_posts, outfile, indent=4)
    


# # Clean each post's text
# cleaned_data = [{"text": clean_text(post["text"]), "engagement": post["engagement"]} for post in raw_data]

# # Save the cleaned data to a new JSON
# cleaned_data



# # Extract metadata for each post


def extract_metadata(post):
    max_retries = 5
    retry_delay = 5  # seconds

    # Template for the post extraction task
    template = '''
    You are given a LinkedIn post. You need to extract number of lines, language of the post, and tags.
    1. Return a valid JSON. No preamble. 
    2. JSON object should have exactly three keys: line_count, language, and tags. 
    3. tags is an array of text tags. Extract a maximum of two tags.
    4. Language should be English or Hinglish (Hinglish means Hindi + English).
    
    Here is the actual post on which you need to perform this task:  
    {post}
    '''

    pt = PromptTemplate.from_template(template)
    # Assuming `llm` is a valid language model chain object.
    chain = pt | llm

    for attempt in range(max_retries):
        try:
            # Invoking the chain to process the post
            response = chain.invoke(input={"post": post})

            # Parsing the JSON response
            json_parser = JsonOutputParser()
            res = json_parser.parse(response.content)
            return res
            
        except HTTPStatusError as e:
            # If too many requests (rate-limiting)
            if e.response.status_code == 429:  # Too Many Requests
                if attempt < max_retries - 1:
                    print(f"Rate limit reached. Retrying in {retry_delay} seconds... (Attempt {attempt + 1}/{max_retries})")
                    time.sleep(retry_delay)  # Wait before retrying
                    continue
                else:
                    raise Exception("Too many requests. Max retries reached.") from e
            else:
                # For other HTTP errors, re-raise the exception
                raise e

        except OutputParserException:
            # Catch if the response couldn't be parsed correctly
            raise OutputParserException("Context too big. Unable to parse post.")
    
    # If the loop finishes without returning, raise a generic exception
    raise Exception("Failed to process the post after retries.")



# def extract_metadata(post):
#     max_retries = 5
#     retry_delay = 5  # seconds

#     # Template for the post extraction task
#     template = '''
#     You are given a LinkedIn post. You need to extract number of lines, language of the post, and tags.
#     1. Return a valid JSON. No preamble. 
#     2. JSON object should have exactly three keys: line_count, language, and tags. 
#     3. tags is an array of text tags. Extract a maximum of two tags.
#     4. Language should be English or Hinglish (Hinglish means Hindi + English).
    
#     Here is the actual post on which you need to perform this task:  
#     {post}
#     '''

#     pt = PromptTemplate.from_template(template)
#     # Assuming `llm` is a valid language model chain object.
#     chain = pt | llm

#     for attempt in range(max_retries):
#         try:
#             # Invoking the chain to process the post
#             print(f"Attempting to process the post (Attempt {attempt + 1}/{max_retries})")
#             response = chain.invoke(input={"post": post})

#             # Debug: print the raw response content
#             print(f"Raw response: {response.content}")

#             # Parsing the JSON response
#             json_parser = JsonOutputParser()
#             res = json_parser.parse(response.content)
            
#             # Debug: print the parsed response
#             print(f"Parsed JSON response: {res}")

#             return res

#         except HTTPStatusError as e:
#             # If too many requests (rate-limiting)
#             if e.response.status_code == 429:  # Too Many Requests
#                 if attempt < max_retries - 1:
#                     print(f"Rate limit reached. Retrying in {retry_delay} seconds... (Attempt {attempt + 1}/{max_retries})")
#                     time.sleep(retry_delay)  # Wait before retrying
#                     continue
#                 else:
#                     raise Exception("Too many requests. Max retries reached.") from e
#             else:
#                 # For other HTTP errors, re-raise the exception
#                 raise e

#         except OutputParserException:
#             # Catch if the response couldn't be parsed correctly
#             print("Error parsing output")
#             raise OutputParserException("Context too big. Unable to parse post.")
    
#     # If the loop finishes without returning, raise a generic exception
#     print("Failed to process the post after retries.")
#     raise Exception("Failed to process the post after retries.")

# # Example call to the function (replace with your actual post content)
# result = extract_metadata("Your LinkedIn post content goes here")
# print(result)  # This will print the final result if successful




    # response = chain.invoke(input={"post": post})

    # try:
    #     json_parser = JsonOutputParser()
    #     res = json_parser.parse(response.content)
    # except OutputParserException:
    #     raise OutputParserException("Context too big. Unable to parse jobs.")
    # return res


def get_unified_tags(posts_with_metadata):
    unique_tags = set()
    # Loop through each post and extract the tags
    for post in posts_with_metadata:
        unique_tags.update(post['tags'])  # Add the tags to the set

    unique_tags_list = ','.join(unique_tags)

    template = '''I will give you a list of tags. You need to unify tags with the following requirements,
    1. Tags are unified and merged to create a shorter list. 
       Example 1: "Jobseekers", "Job Hunting" can be all merged into a single tag "Job Search". 
       Example 2: "Motivation", "Inspiration", "Drive" can be mapped to "Motivation"
       Example 3: "Personal Growth", "Personal Development", "Self Improvement" can be mapped to "Self Improvement"
       Example 4: "Scam Alert", "Job Scam" etc. can be mapped to "Scams"
    2. Each tag should be follow title case convention. example: "Motivation", "Job Search"
    3. Output should be a JSON object, No preamble
    3. Output should have mapping of original tag and the unified tag. 
       For example: {{"Jobseekers": "Job Search",  "Job Hunting": "Job Search", "Motivation": "Motivation}}
    
    Here is the list of tags: 
    {tags}
    '''
    pt = PromptTemplate.from_template(template)
    chain = pt | llm
    response = chain.invoke(input={"tags": str(unique_tags_list)}) # Invoke the chain with the tags--> what it does is it takes the tags and then it unifies them
        
    try:
        json_parser = JsonOutputParser()
        res = json_parser.parse(response.content)
    except OutputParserException:
        raise OutputParserException("Context too big. Unable to parse jobs.")
    return res


if __name__ == "__main__":
    process_posts("data/raw_posts.json", "data/processed_posts.json")