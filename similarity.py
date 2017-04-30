import string

from nltk.corpus import stopwords

def vectorize(text):
    '''
    Function to clean and vectorize given text

    Input: single string

    Output: vector of selected words
    '''
    lowers = text.lower()
    
    translator = str.maketrans('', '', string.punctuation)

    no_punctuation = lowers.translate(translator)

    #split
    text_list = no_punctuation.strip().split()

    vector_text = [w for w in text_list if not w in stopwords.words('english')]

    return vector_text

def similarity(text0, text1):
    '''
    Function to calculate the similarity between two vector texts
    '''
    return abs(len(text0) - len(text1))

def main():

    with open('outputfile.txt', 'r') as f:
        count = 0
        
        for line in f:
            #print(line[:20])
            business_id0, review_id0, text0, business_id1, review_id1, text1  = line.split("|")
            
            vector_text0 = vectorize(text0)
            vector_text1 = vectorize(text1)

            similarity_score = similarity(vector_text0, vector_text1)

            print(review_id0, "vs", review_id1, ":")
            print(similarity_score)

            count += 1

    print(count)

if __name__ == '__main__':

    main()