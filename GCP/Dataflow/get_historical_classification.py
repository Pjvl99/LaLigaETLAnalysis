from apache_beam.options.pipeline_options import PipelineOptions
import numpy as np
from google.cloud import storage
import pyarrow
import apache_beam as beam

class Getting_historical_classification(beam.DoFn):
    
    def __init__(self):
        self.columns = ['position', 'team', 'pts', 'pl', 'w', 'd', 'l', 'f', 'a', 'season']
        self.columns_2 = ['position', 'team', 'pl', 'w', 'd', 'l', 'f', 'a', 'pts', 'season']
    
    def process(self, element):
        season = element['season'][0]
        if len(element.columns) > 12:
            self.upload_historical_results_to_gcs(element, season)
    
        element = self.clean_element(element, season)
        for _, row in element.iterrows():
            row['position'] = int(row['position'])
            row['pts'] = int(row['pts'])
            row['pl'] = int(row['pl'])
            row['w'] = int(row['w'])
            row['d'] = int(row['d'])
            row['l'] = int(row['l'])
            row['f'] = int(row['f'])
            row['a'] = int(row['a'])
            yield row.to_dict()
            
    def clean_element(self, element, season):
        element = element.iloc[:, 0:11]
        elements_to_delete = [" (D)", " (C)", "[a]", "[b]", "[c]", "[d]", "[Nota 1]", "[Nota 2]"]
        if "pts" in element.columns[2].lower():
            element = element.iloc[:, 0:9]
            element['season'] = season
            element.columns = self.columns
        elif "pts" in element.columns[9].lower():
            element = element.drop(element.columns[8], axis=1)
            element.columns = self.columns_2
        elif "pts" in element.columns[10].lower():
            element = element.drop(element.columns[[8,9]], axis=1)
            element['season'] = season
            element.columns = self.columns_2
        element['team'] = element['team'].str.split(' \[', expand=True)[0]
        for element_to_delete in elements_to_delete:        
            element['team'] = element['team'].str.replace(element_to_delete, "")
        element['pts'] = element['pts'].astype(str).str.extract(r'(\d+)')
        return element

    def upload_historical_results_to_gcs(self, element, season):
        historical_positions = element.iloc[:, 12:len(element.columns)-1]
        value_list = []
        value_list.extend(element.iloc[:, 1].tolist())
        value_list.append('local')
        historical_positions['local'] = element['Equipo']
        historical_positions.columns = value_list
        historical_positions = historical_positions.melt(id_vars=['local'], var_name='away', value_name='result')
        historical_positions = historical_positions[(historical_positions['local'] != historical_positions['away'])]
        historical_positions[['local_goals', 'away_goals']] = historical_positions['result'].str.split('–', n=1, expand=True).astype(int)
        historical_positions['season'] = season
        blob = self.bucket.blob(f"laliga_output/results/historical_results{season}")
        blob.upload_from_string(historical_positions.to_parquet(), 'application/octet-stream')
        
    def setup(self):
        client = storage.Client()
        self.bucket = client.bucket("trim-heaven-415202")
            
output_gcs = 'gs://trim-heaven-415202/laliga_output/hitorical_classification/classification'
with beam.Pipeline(options=PipelineOptions()) as p: 
    df = (p 
     | 'Read parquet' >> beam.io.ReadFromParquetBatched(f"gs://trim-heaven-415202/laliga/classification/*")
     | 'Convert to pandas' >> beam.Map(lambda table: table.to_pandas())
     | 'Transform dataframes' >> beam.ParDo(Getting_historical_classification())
     | 'Uploading to GCS' >> beam.io.WriteToParquet(output_gcs, pyarrow.schema(
                                                  [('position', pyarrow.int64()), 
                                                   ('pts', pyarrow.int64()), 
                                                   ('pl', pyarrow.int64()),
                                                   ('w', pyarrow.int64()),
                                                   ('d', pyarrow.int64()),
                                                   ('l', pyarrow.int64()),
                                                   ('f', pyarrow.int64()),
                                                   ('a', pyarrow.int64()),
                                                   ('team', pyarrow.string()),
                                                   ('season', pyarrow.string())]
     )))