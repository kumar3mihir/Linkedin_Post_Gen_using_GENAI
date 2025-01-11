import json

def process_posts(raw_file_path, processed_file_path = "Data/processed_data.json"):
    with open(raw_file_path, encoding='utf-8') as file:
        posts = json.load(file)
        for post in posts:
            extract_metadata(post['text'])
    
if __name__ == "__main__":
        process_posts("Data/raw_posts.json", "Data/processed_posts.json")