import scrapy
import pandas as pd
from google.cloud import storage
import os

class LaLiga(scrapy.Spider):

    name = "laliga"
    project_id = os.environ["PROJECT_ID"]
    bucket_value = os.environ["BUCKET_NAME"]
    start_urls = ["https://es.wikipedia.org/wiki/Primera_Divisi%C3%B3n_de_Espa%C3%B1a"]
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_value)

    def parse(self, response):
        if response.status == 200:
            table = response.css('table[class^="sortable col2izq col3izq"]')
            hrefs = table.css("tbody > tr > td:first-child > a").xpath("@href").getall()
            for href in hrefs:
                yield response.follow(href, callback=self.get_laliga_statistics)
        else:
            print("Error in connection:", response.url)
    
    def upload_data_to_gcs(self, season, table, folder, input_df=pd.DataFrame()) -> bool:
        blob = self.bucket.blob(f"laliga/{folder}/{season}.parquet")
        if len(input_df) == 0:
            df_list = pd.read_html(table.get())
            df = df_list[0]
            df['season'] = season
            blob.upload_from_string(df.to_parquet(), 'application/octet-stream')
        else:
            input_df['season'] = season
            blob.upload_from_string(input_df.to_parquet(), 'application/octet-stream')
        return True

    def get_laliga_statistics(self, response):
        if response.status == 200:
            tables = response.css("table")
            classification_table_exists = False
            max_scorers_exists = False
            historical_positions_exists = False
            season = response.url[-7:]
            for table in tables:

                if not max_scorers_exists: 
                    scorers_value = table.css("tbody > tr > th:nth-child(3)") #Scorers
                    if scorers_value and "oles" in scorers_value.get():
                        max_scorers_exists = self.upload_data_to_gcs(season=season, table=table, folder="scorers")
                    else:
                        scorers_value = table.css("tbody > tr > th:nth-child(4)")
                        if scorers_value and "oles" in scorers_value.get():
                            max_scorers_exists = self.upload_data_to_gcs(season=season, table=table, folder="scorers")

                value = table.css("tbody > tr > th:first-child::text").get() #Results
                if value and "ocal" in value and "isitante" in value:
                    self.upload_data_to_gcs(season, table, folder="results")
            
                table_class = table.xpath("@class").get() #Get historical position
                if table_class and "wikitable" in table_class: 
                    df_list = pd.read_html(table.get())
                    df = df_list[0]
                    df['season'] = season
                    if len(df) >= 10 and len(df.columns) >= 18 and not historical_positions_exists:
                        historical_positions_exists = self.upload_data_to_gcs(season=season, table=table, folder="historical_position", input_df=df)
            
                table_attribute = table.css("tbody > tr:first-child > th:nth-child(2)::text").get()    # Get classification table
                if table_attribute and not classification_table_exists and "quipo" in table_attribute:
                    df_list = pd.read_html(table.get())
                    df = df_list[0]
                    df['season'] = season
                    if len(df) >= 10:
                        classification_table_exists = self.upload_data_to_gcs(season=season, table=table, folder="classification", input_df=df)
        else:
            print("Error in connection:", response.url)