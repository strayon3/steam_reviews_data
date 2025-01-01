#libs needed for processing data 
import pandas as pd 
import plotly.express as px
import re 
from techissuelist import techissues
from largedatasetreader import parallel_read_csv
from largedatasetreader import process_game
from concurrent.futures import ThreadPoolExecutor as te
import gc

'''We are going to be looking at steam game reviews and processing the data '''
title_counter = 0
game_titles = []



if __name__ == '__main__':
    file_path = "./steam_reviews.csv"
    dataframe = parallel_read_csv(file_path)
    
    #Validate the result
    print(f"Loaded Dataframe with {len(dataframe)} rows")
    print(dataframe.head())
    dataframe = dataframe.to_pandas()

#sort and creat a catagory for each game 
    game_titles = []
    for i in dataframe["app_name"]:
        if i not in game_titles:
            game_titles.append(i)
        

#look reviews and determin sentiments
with open("c:/Users/stray/Desktop/game_reviews_wordlist/positive_sentiment_words.txt", "r") as f:
   good_keywords = [line.strip() for line in f.readlines()]

#load bad wordlist
with open("c:/Users/stray/Desktop/game_reviews_wordlist/negative_sentiment_words.txt","r") as f:
    bad_keywords = [line.strip() for line in f.readlines()]
#define bad keyword lists
    game_sentiment = {}

#testing this function
#sentiment analysis function that will store game title and sentiment score in a dict for graph 
#testing this function
#sentiment analysis function that will store game title and sentiment score in a dict for graph 
def analyze_review(dataframe, game_titles):
    game_scores = {title: {"Good": 0, "Bad": 0} for title in game_titles}
    chunk_size = 100000 #process 20 titles at a time

    #set regex search filters
    good_patern = re.compile(r'\b(?:' + '|'.join(map(re.escape, good_keywords)) + r')\b', re.IGNORECASE) 
    bad_patern = re.compile(r'\b(?:' + '|'.join(map(re.escape, bad_keywords)) + r')\b', re.IGNORECASE)

    with te() as executor:
        executor.map(lambda game: process_game(game, dataframe, game_scores, good_patern, bad_patern, chunk_size), game_titles)

    
    return game_scores
#calculate sentiment percent for each game
def calculate_percents(game_scores):
     sentiment_results = {}

     for game, scores in game_scores.items():
          total_reviews = scores["Good"] + scores["Bad"]

          if total_reviews > 0:
               sentiment_score = (scores["Good"] - scores["Bad"]) / total_reviews
               sentiment_results[game] = {
                    "Sentiment Score": round(sentiment_score, 2),
                    "Good": scores["Good"],
                    "Bad": scores["Bad"]
               }
          else:
               sentiment_results[game] = {
                    "Sentiment Score": 0,
                    "Good": scores["Good"],
                    "Bad": scores["Bad"]
               }
     return sentiment_results

# Usage:
game_sentiment = analyze_review(dataframe,game_titles)
games = list(game_sentiment.keys())
good_reviews = [game_sentiment[game]["Good"] for game in games]
bad_reviews = [game_sentiment[game]["Bad"] for game in games]

#call calculate_percents function and store the results 
sentiment_scores = calculate_percents(game_sentiment)
sentiment_values = [sentiment_scores[game]["Sentiment Score"] for game in games]


#create the graphs 

fig = px.bar(
        x=games,
        y=[good_reviews,bad_reviews],
        labels={"x":"Game", "y":"Review Count"}, #Axis labels
        title="Good vs. Bad Reviews per Game",
        barmode="group"  #grouped bars for comparison
    )

#display the figure
fig.show()



#search the reviews for common tech issues to see what ammount of reviews are related to tech issues 
bugcomplaints = dataframe["review"]

#initialize a dictionary to count mentions
issue_count = {title: {issue : 0 for issue in techissues.keys()} for title in game_titles}

#analyze reviews
for title,review in zip(game_titles,bugcomplaints):
        for issues, keywords in techissues.items():
            if any(re.search(rf"{re.escape(keyword)}", review.lower()) for keyword in keywords ):
                issue_count[title][issues] += 1

        print("Tech Issue Mentions By Game:")
for title, issues in issue_count.items():
        print(f"\n{title}: ")
        for issue,count in issues.items():
            if count > 0:
                print(f"   {issue}: {count} mentions of this Issue")
        