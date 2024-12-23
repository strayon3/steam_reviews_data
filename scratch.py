for col, row in dataframe.iterrows():
        gameT = row["app_name"]
        review = row["review"]
        good_score,bad_score = analyze_review(review)

        if gameT not in game_sentiment:
            game_sentiment[gameT] = {"Good": 0, "Bad": 0}

#increment the count based on good or bad 
        if good_score > bad_score:
            game_sentiment[gameT]["Good"] += 1
        elif bad_score > good_score:
            game_sentiment[gameT]["Bad"] += 1
        elif good_score == 0 and bad_score == 0:
        #Neutral review skip increment
            continue
        else:
            game_sentiment[gameT]["Bad"] += 1
            game_sentiment[gameT]["Good"] += 1








def parallel_read_csv(file_path,chunk_size=999999):
    chunks = pd.read_csv(file_path,chunksize=chunk_size)
    results = []


    #use parallelpool for parallel proccesing 
    with ppe(max_workers=os.cpu_count()) as executor:
        #map each chunk to the process function 
        for result in executor.map(process_chunk,chunks):
                results.append(result)#Append each chunk
        gc.collect() #clean up memory by freeing unused objects
    
    #combine all the processed chunkcs into single datadrame 
    dataframe = pd.concat(results, ignore_index=True)
    return dataframe







def analyze_review(review):
        words = re.findall(r'\b\w+\b', review.lower()) #Covert to lowercase for case-insensitive matching 
        good_score = sum(1 for word in good_keywords if word in words)
        bad_score = sum(1 for word in bad_keywords if word in words)
        return good_score, bad_score

    #check for good and bad section
    for row in dataframe.iter_rows(named=True):
        gameT = row["app_name"]
        review = row["review"]
        good_score,bad_score = analyze_review(review)

        if gameT not in game_sentiment:
            game_sentiment[gameT] = {"Good": 0, "Bad": 0}

#increment the count based on good or bad 
        if good_score > bad_score:
            game_sentiment[gameT]["Good"] += 1
        elif bad_score > good_score:
            game_sentiment[gameT]["Bad"] += 1
        elif good_score == 0 and bad_score == 0:
        #Neutral review skip increment
            continue
        else:
            game_sentiment[gameT]["Bad"] += 1
            game_sentiment[gameT]["Good"] += 1

#display each game and their review sentiment count 
    for game,sentiments in game_sentiment.items():
        print(f"\n{game}: {sentiments}\n")
