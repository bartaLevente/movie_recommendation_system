from database_utils import get_titles, update_clean_titles
import re

def clean_title(title):
    return re.sub("[^a-zA-Z0-9 ]","",title)


titles_df = get_titles()
titles_df["clean_titles"] = titles_df["title"].apply(clean_title)
clean_titles_tuples = list(zip(titles_df["clean_titles"], titles_df["movieId"]))

update_clean_titles(clean_titles_tuples)
