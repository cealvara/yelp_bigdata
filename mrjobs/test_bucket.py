from google.cloud import storage

if __name__ == '__main__':
    client = storage.Client()
    bucket = client.get_bucket('data-cs123')
    blob = bucket.get_blob('pos_words.txt')
    print(blob.download_as_string())