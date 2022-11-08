import numpy as np
import spacy
import pandas as pd
from tqdm import tqdm
from spacy import displacy
import plotly.express as px


nlp = spacy.load('it_core_news_lg')

# Initial data cleanup and filtering
# TODO: fix hardcoded column names

def clean_data(path):
    input = pd.read_csv(path)
    data = input[['id', 'contract', 'macrozone', 'neighbourhood', 'price.value', 'description']]
    # remove auction listings from the dataset
    data = data.loc[data['contract'] == 'sale']
    # make all descriptions lowercase
    data.loc[:, 'description'] = data.loc[:, 'description'].str.lower()
    # remove all numbers, newline, and \tab characters from description column in df
    data.loc[:, 'description'] = data.loc[:, 'description'].str.replace('\n', ' ')
    data.loc[:, 'description'] = data.loc[:, 'description'].str.replace('\t', ' ')
    # strip punctuation from description column in df
    data.loc[:, 'description'] = data.loc[:, 'description'].str.strip('!@#$%^&*()_+-=[]{};\':"|./<>?')
    data.loc[:, 'description'] = data.loc[:, 'description'].str.replace('[^\w\s]', ' ')
    # remove all duplicate spaces in description column in df
    data.loc[:, 'description'] = data.loc[:, 'description'].str.replace(' +', ' ')
    # remove rows with missing values
    data = data.dropna()
    return data


# Create new dataset with text tokens from description column
def tag_data(df):
    rows = []
    print('Processing tokens...')
    for i, item in tqdm(df.iterrows(), total=df.shape[0]):
        doc = nlp(item['description'])
        for token in doc:
            rows.append({'id': item['id'],
                         'macrozone': item['macrozone'],
                         'neighbourhood': item['neighbourhood'],
                         'price': item['price.value'],
                         'text': token.text,
                         'ent_type': token.ent_type_,
                         'lemma': token.lemma_,
                         'pos': token.pos_,
                         'tag': token.tag_,
                         'dep': token.dep_,
                         'shape': token.shape_,
                         'is_alpha': token.is_alpha,
                         'is_stop': token.is_stop})
    return pd.DataFrame(rows)


# tag_data(clean_data('data_sales.csv')).to_csv('data_tagged.csv', index=False, encoding='utf-8')
data = pd.read_csv('data_tagged.csv')
print('Data loaded.')


# optimise the following code
def most_used_words_by_neighbourhood(data):
    neighbourhoods = data['neighbourhood'].unique()
    words_df = pd.DataFrame(columns=list(neighbourhoods))
    for neighbourhood in neighbourhoods:
        n_data = data.loc[data['neighbourhood'] == neighbourhood]
        words = n_data.loc[(data['is_stop'] == False) & (data['is_alpha'] == True)].groupby(
            ['lemma']).size().reset_index(
            name='count').sort_values(by='count', ascending=False).dropna().reset_index(drop=True)
        words_df.loc[:, neighbourhood] = words['lemma']
    return words_df


def most_used_words_by_price(data):
    # create a dataframe with three columns: word, frequency_above_mean, frequency_below_mean
    words_df = pd.DataFrame(columns=['word', 'frequency_above_mean', 'frequency_below_mean'])
    # from data, select only rows with is_alpha == True and is_stop == False
    data = data.loc[(data['is_alpha'] == True) & (data['is_stop'] == False)]
    # select only rows where price is within 1.5 standard deviations of mean
    data = data.loc[(data['price'] > data['price'].mean() - 1.5 * data['price'].std()) & (
            data['price'] < data['price'].mean() + 1.5 * data['price'].std())]
    # count how many unique words are used for the rows above and below the mean
    words_df.loc[:, 'frequency_above_mean'] = data.loc[data['price'] > data['price'].mean(), 'lemma'].value_counts()
    words_df.loc[:, 'frequency_below_mean'] = data.loc[data['price'] < data['price'].mean(), 'lemma'].value_counts()
    # create a set with all the words in the data
    words = set(data['lemma'])
    # for each word in the set create a row in the dataframe 'words_df'
    # calculate the frequency of the word in the data above the mean
    words_df.loc[:, 'frequency_above_mean'] = data.loc[data['price'] > data['price'].mean(), 'lemma' ].value_counts()/data.loc[data['price'] > data['price'].mean(), 'lemma'].shape[0]
    # calculate the frequency of the word in the data below the mean
    words_df.loc[:, 'frequency_below_mean'] = data.loc[data['price'] < data['price'].mean(), 'lemma'].value_counts()/data.loc[data['price'] < data['price'].mean(), 'lemma'].shape[0]
    words_df['word'] = words_df.index
    words_df.to_csv('words_df.csv', index=False, encoding='utf-8')
    return words_df


def plot_words_by_price(path):
    df = pd.read_csv(path)
    df['frequency_above_mean'] = df['frequency_above_mean'].apply(lambda x: np.log10(x))
    df['frequency_below_mean'] = df['frequency_below_mean'].apply(lambda x: np.log10(x))
    fig = px.scatter(df, x="frequency_below_mean", y="frequency_above_mean", hover_data=['word'])
    fig.show()

def keyword_percentage_by_neighbourhood(data):
    cln_data = data.loc[(data['is_alpha'] == True) & (data['is_stop'] == False) & (data['shape'] == 'xxxx')]
    neighbourhoods = data['neighbourhood'].unique()
    total_words_by_neighbourhood = cln_data.groupby(['neighbourhood']).count()
    individual_word_counts_by_neighbourhood = cln_data.groupby(['neighbourhood', 'text']).size().reset_index()
    # percent_by_neighbourhood = pd.DataFrame(columns=list(neighbourhoods))
    # percent_by_neighbourhood = individual_word_counts_by_neighbourhood.loc['neighbourhood']/total_words_by_neighbourhood.loc['neighbourhood']
    word_list = set(data['text'])
    word_percentage_by_neighbourhood = pd.pivot_table(individual_word_counts_by_neighbourhood,
                                                       index='neighbourhood', columns='text').fillna(0)
    # divide every cell in word_percentage_by_neighbourhood by the total number of words in the neighbourhood
    for idx, row in word_percentage_by_neighbourhood.iterrows():
        word_percentage_by_neighbourhood.loc[idx] = word_percentage_by_neighbourhood.loc[idx].div(total_words_by_neighbourhood.loc[idx, 'text'], axis=0)
    return word_percentage_by_neighbourhood

def extract_word_col(path, keyword):
    df = pd.read_csv(path).loc[:,['neighbourhood', keyword]]
    #df.to_csv('{keyword}_frequency_by_neighbourhood.csv', index=False, encoding='utf-8')
    return df

#x = keyword_percentage_by_neighbourhood(data)
#x.to_csv('word_percentage_by_neighbourhood.csv', index=True, encoding='utf-8')
#y = extract_word_col('word_percentage_by_neighbourhood.csv', 'verde')
x = plot_words_by_price('words_df.csv')
print(x)
