import pandas as pd 
import polars as ppl
import os
import gc




def parallel_read_csv(file_path, chunk_size=999999):
     """
    Read a large CSV file in parallel using chunks and process each chunk.
    
    Args:
    - file_path: Path to the CSV file
    - chunk_size: Size of the chunks to read at once (default: 999999)
    
    Returns:
    - A concatenated dataframe containing the processed data
    """
     #Lazy scan the csv file 
     dataframe = ppl.scan_csv(file_path).select([
          ppl.col("app_name"),
          ppl.col("language"),
          ppl.col("review")
     ]).filter(
          (ppl.col("language") == "english") &
          (ppl.col("review").is_not_null()) 
          #(ppl.col("review").str.len() >=3)
     )

     #dataframe = ppl.scan_csv(file_path, with_column_names=True, n_rows=chunk_size)

     results = dataframe.collect()

    
    #clean up memory for speed nothing else 
     gc.collect()

     return results