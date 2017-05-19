from google.cloud import storage

if __name__ == '__main__':
    client = storage.Client()
    bucket = client.get_bucket('data-cs123')
    blob = bucket.get_blob('pos_words.txt')
    positive_words = blob.download_as_string().splitlines()
    
    if 'approval' in positive_words:
        print(positive_words)
