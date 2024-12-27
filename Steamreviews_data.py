#libs needed for processing data 
import pandas as pd 
import plotly.express as px
import re 
from techissuelist import techissues
from largedatasetreader import parallel_read_csv
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
    good_keywords = "c:/Users/stray/Desktop/portfolioDatasets/datasets/wordlists/positive words.csv"
    bad_keywords = "c:/Users/stray/Desktop/portfolioDatasets/datasets/wordlists/archive/negative-words.txt"
#define bad keyword lists
    game_sentiment = {}



#Display each game and their avg sentiment score
for game, sentiments in game_sentiment.items():
    print(f"\n{game}: {sentiments}\n")

#testing this function
#sentiment analysis function that will store game title and sentiment score in a dict for graph 
def analyze_review(dataframe):
    game_scores = {}
    chunk_size = 20 #process 20 titles at a time 

    #group by app_name
    grouped = dataframe.groupby("app_name")
    # Process reviews one at a time
    for game_name,game_data in grouped:
        game_scores[game_name] = {"Good": 0, "Bad": 0}
    
    #process this games reviews in chunks
    for chunk_start in range(0,len(game_data),chunk_size):
        chunk_end = chunk_start + chunk_size
        chunk = game_data.iloc[chunk_start:chunk_end]

        #process reviews for current chunk 
        for col, row in chunk.iterrows():
            game = row["app_name"]
            review = str(row["review"]).lower()


        # Count scores for this review
            good_count = sum(1 for word in review.split() if word in good_keywords)
            bad_count = sum(1 for word in review.split() if word in bad_keywords)
        
        # Add to game's total
            game_scores[game]["Good"] += good_count
            game_scores[game]["Bad"] += bad_count
        del chunk
        gc.collect()
    return game_scores

# Usage:
game_sentiment = analyze_review(dataframe)
games = list(game_sentiment.keys())
good_reviews = [game_sentiment[game]["Good"] for game in games]
bad_reviews = [game_sentiment[game]["Bad"] for game in games]


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
        