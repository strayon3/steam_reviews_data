#libs needed for processing data 
import pandas as pd 
import plotly.express as px
import re 
import polars as ppl
from techissuelist import techissues
from largedatasetreader import parallel_read_csv
'''We are going to be looking at steam game reviews and processing the data '''
title_counter = 0
game_titles = []



if __name__ == '__main__':
    file_path = "./steam_reviews.csv"
    dataframe = parallel_read_csv(file_path)

    #Validate the result
    print(f"Loaded Dataframe with {len(dataframe)} rows")
    print(dataframe.head())


#sort and creat a catagory for each game 
    for i in dataframe["app_name"]:
        if i not in game_titles:
            game_titles.append(i)


#look reviews and determin sentiments 
    good_keywords = "c:/Users/stray/Desktop/portfolioDatasets/datasets/wordlists/positive words.csv"
    bad_keywords = "c:/Users/stray/Desktop/portfolioDatasets/datasets/wordlists/archive/negative-words.txt"
#define bad keyword lists
    game_sentiment = {}

    def analyze_review(dataframe):
        reviews = dataframe["review"].str.to_lowercase().str.extract_all(r'\b\w+\b')

        #calculates good and bad scores
        good_score = reviews.apply(lambda words: sum(1 for word in good_keywords if word in words))
        bad_score = reviews.apply(lambda words: sum(1 for word in bad_keywords if word in words))

        #Add scores to the dataframe
        dataframe = dataframe.with_column([
            ppl.Series("good_score", good_score),
            ppl.Series("bad_score", bad_score)
        ])
        return dataframe
    
    #analyze reviews
    dataframe = analyze_review(dataframe)

    #aggragate the scores
    aggregate = dataframe.groupby("app_name").agg([
        ppl.col("good_score").mean().alias("good_score"),
        ppl.col("bad_score").mean().alias("bad_score")
    ])

    #convert to dict
    for row in aggregate.iter_rows(named=True):
        gameT = row["app_name"]
        good_score = row["good_score"]
        bad_score = row["bad_score"]
        game_sentiment[gameT] = {
            "Good score": good_score,
            "Bad score": bad_score
        }
        

#Display each game and their avg sentiment score
for game, sentiments in game_sentiment.items():
    print(f"\n{game}: {sentiments}\n")






#break game_sentiment apart into three lists 
    games = list(game_sentiment.keys())  #Game titles (x-axis)
    good_reviews = [game_sentiment[game]["Good"] for game in games] #good count (y-axis)
    bad_reviews = [game_sentiment[game]["Bad"] for game in games] #Bad counts ( y-axis)

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
        