import pandas as pd
import numpy as np
import streamlit as st
from database_utils import get_similar_recs, get_all_user_recs, to_python_type, create_indexes

@st.cache_data(ttl=3600)
def load_movies():
    from database_utils import get_movies
    return get_movies()

@st.cache_data(ttl=600)
def get_cached_similar_recs(movie_id):
    return get_similar_recs(movie_id)

@st.cache_data(ttl=600)
def get_cached_all_user_recs(movie_id):
    return get_all_user_recs(movie_id)

def recommendation(movie_id):
    try:
        # Convert NumPy int64 to Python int
        movie_id = to_python_type(movie_id)
        
        # Get data with caching
        similar_user_recs = get_cached_similar_recs(movie_id)
        all_users = get_cached_all_user_recs(movie_id)
        
        # Calculate all_user_recs
        if not all_users.empty:
            all_user_recs = all_users["movieId"].value_counts() / len(all_users["userId"].unique())
            all_user_recs = all_user_recs.reset_index()
            all_user_recs.columns = ['movieId', 'count']
            
            # Merge recommendations
            combined_recommendations = pd.merge(similar_user_recs, all_user_recs, on='movieId', how='outer')
            combined_recommendations = combined_recommendations.fillna(0)
            
            # Avoid division by zero
            combined_recommendations["score"] = combined_recommendations["similarity_score"] / combined_recommendations["count"].replace(0, 1)
            combined_recommendations = combined_recommendations.sort_values("score", ascending=False)
            
            # Join with movies table to get titles
            results = pd.merge(combined_recommendations.head(10), load_movies(), on="movieId")
            return results
        
        return pd.DataFrame()
    
    except Exception as e:
        st.error(f"Error in recommendation function: {e}")
        return pd.DataFrame()

def main():
    st.set_page_config(page_title="Movie Recommender", layout="wide")
    st.title("Movie Recommendation System")
    
    # Get a list of movies for selection
    movies = load_movies()
    
    # Create a selection box of movie titles
    movie_titles = movies['title'].tolist()
    selected_movie = st.selectbox("Select a movie", movie_titles)
    
    if selected_movie:
        selected_movie_id = movies[movies['title'] == selected_movie]['movieId'].iloc[0]
        
        # Button to get recommendations
        if st.button("Get Recommendations"):
            try:
                with st.spinner('Finding recommendations...'):
                    # Get recommendations
                    recommendations = recommendation(selected_movie_id)
                
                if not recommendations.empty:
                    # Display recommendations
                    st.subheader(f"Top 10 Recommendations for '{selected_movie}'")
                    
                    # Create a display for recommendations
                    for i, (index, row) in enumerate(recommendations.iterrows(), 1):
                        with st.container():
                            col1, col2 = st.columns([3, 1])
                            with col1:
                                st.write(f"**{i}. {row['title']}**")
                            with col2:
                                st.write(f"Score: {row['score']:.4f}")
                            st.divider()
                else:
                    st.warning("No recommendations found for this movie.")
            
            except Exception as e:
                st.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()