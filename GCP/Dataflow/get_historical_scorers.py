from apache_beam.options.pipeline_options import PipelineOptions
import numpy as np
import pandas as pd
import pyarrow
import apache_beam as beam

bucket_name = "trim-heaven-415202"

class Getting_historical_scorers(beam.DoFn):
    
    def __init__(self):
        self.pattern = r'(\d+)'
        self.columns = ['position', 'player', 'goals']
    
    def process(self, element):
        season = element['season'][0]
        element = element.drop(columns=['season'])
        element = self.clean_element(element, season)
        for _, row in element.iterrows():
            row['goals'] = int(row['goals'])
            row['real_position'] = int(row['real_position'])
            yield row.to_dict()
            
    def clean_element(self, element, season):
        if len(element.columns) == 3:
            element.columns = self.columns
            element['goals'] = element['goals'].str.replace('[1]', '')
            element['goals'] = pd.to_numeric(element['goals'], errors='coerce')
            element['season'] = season
        else:
            element = element.iloc[:, 0:4]
            if element.iloc[0:1, 2].dtype == 'int64' and 'G' in element.columns[2].upper():
                element = element.iloc[:, 0:3]
            else:
                element = element.drop(element.columns[2], axis=1)
            element.columns = self.columns
            element['season'] = season
            element['goals'] = element['goals'].astype(str).str.extract(self.pattern)
            element['goals'] = pd.to_numeric(element['goals'], errors='coerce')
        if len(str(element['position'][2])) > 4:
            element['player'] = element['position']
        element = element.dropna(subset=['goals'])
        element = element.drop(columns=['position'])
        element['real_position'] = range(1, len(element['goals'])+1)
        return element

output_gcs = f'gs://{bucket_name}/laliga_output/scorers/historical_scorers'
with beam.Pipeline(options=PipelineOptions()) as p: 
    df = (p 
     | 'Read parquet' >> beam.io.ReadFromParquetBatched(f"gs://{bucket_name}/laliga/scorers/*")
     | 'Convert to pandas' >> beam.Map(lambda table: table.to_pandas())
     | 'Transform Columns' >> beam.ParDo(Getting_historical_scorers())
     | 'Uploading to GCS' >> beam.io.WriteToParquet(output_gcs, pyarrow.schema(
                                                  [('player', pyarrow.string()), 
                                                   ('goals', pyarrow.int64()), 
                                                   ('season', pyarrow.string()),
                                                   ('real_position', pyarrow.int64())]
     )))