import nltk

def clean(text):
    '''
    Function to clean given text
    '''
    return text

def similarity(text0, text1):
    '''
    Function to calculate the similarity between two texts
    '''
    return abs(len(text0) - len(text1))

def main():

    with open('outputfile.txt', 'r') as f:
        count = 0
        
        for line in f:
            business_id0, review_id0, text0, business_id1, review_id1, text1  = line.split("|")
            count += 1
            
            clean_text0 = clean(text0)
            clean_text1 = clean(text1)

            similarity = get_similarity(clean_text0, clean_text1)


    print(count)

if __name__ == '__main__':

    main()