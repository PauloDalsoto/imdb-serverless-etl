import pandas as pd

class SilverToGoldProcessor:
    def __init__(self, s3_service, source_bucket, target_bucket):
        self.s3 = s3_service
        self.source_bucket = source_bucket
        self.target_bucket = target_bucket

    def process(self, key):
        normalized_data = self.s3.load_csv(self.source_bucket, key)
        if normalized_data.empty:
            raise Exception("No data to process, empty csv file!")
        
        normalized_data['rank'] = normalized_data['rank'].astype(int)
        normalized_data = normalized_data.sort_values(by='rank')
        
        self.process_analytics(normalized_data)

        return len(normalized_data)
    
    def process_analytics(self, df):
        prefix = "gold/"

        try:
            # 1. Top N IMDb Ratings
            topN_rated_df = (
                df[[
                    'rank', 'title', 'year', 'imdbrating', 'imdbratingcount',
                    'released', 'runtime', 'genre', 'director', 'language',
                    'country', 'awards', 'metascore', 'imdbvotes', 'boxoffice'
                ]]
                .dropna()
                .sort_values(by='imdbrating', ascending=False)
                .head(10)
            )
            self.s3.save_csv(self.target_bucket, f"{prefix}topN_rated.csv", topN_rated_df.to_csv(index=False))

            # 2. Genre Distribution
            genre_series = df['genre'].dropna().str.split(', ').explode()
            movies_by_genre_df = genre_series.value_counts().rename_axis('genre').reset_index(name='count')
            self.s3.save_csv(self.target_bucket, f"{prefix}movies_by_genre.csv", movies_by_genre_df.to_csv(index=False))

            # 3. movies per country
            df['country'] = df['country'].fillna('').astype(str)
            countries_exploded = df['country'].str.split(',', expand=True).stack().str.strip()
            country_counts = countries_exploded.value_counts().reset_index()
            country_counts.columns = ['Country', 'MovieCount']
            country_counts = country_counts.sort_values(by='MovieCount', ascending=False)
            self.s3.save_csv(self.target_bucket, f"{prefix}movies_by_country.csv", country_counts.to_csv(index=False))

            # 4. Movies per year
            movies_per_year_df = (
                df['year'].dropna().astype(int).value_counts().sort_index(ascending=False).rename_axis('year').reset_index(name='count')
            )
            self.s3.save_csv(self.target_bucket, f"{prefix}movies_per_year.csv", movies_per_year_df.to_csv(index=False))
            
            # 5. Total Box Office Revenue per Year
            df['boxoffice_clean'] = (
                df['boxoffice'].astype(str)
                .str.replace('$', '', regex=False)
                .str.replace(',', '', regex=False)  
                .replace('nan', '0') 
                .astype(int)
            )
            box_office_df = (
                df.groupby('year', as_index=False)['boxoffice_clean']
                .sum()
                .rename(columns={'boxoffice_clean': 'total_box_office'})
                .sort_values(by='year', ascending=False)
            )
            self.s3.save_csv(self.target_bucket, f"{prefix}box_office_per_year.csv", box_office_df.to_csv(index=False))

            # 6. Top 5 Directors by Movie Count
            director_series = df['director'].dropna().str.split(', ').explode()
            top_directors_df = (
                director_series.value_counts().head(5).rename_axis('director').reset_index(name='movie_count')
            )
            self.s3.save_csv(self.target_bucket, f"{prefix}top_directors.csv", top_directors_df.to_csv(index=False))

        except Exception as e:
            raise Exception(f"Error processing analytics: {e}")