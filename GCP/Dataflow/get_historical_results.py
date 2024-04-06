from apache_beam.options.pipeline_options import PipelineOptions
import numpy as np
import pyarrow
import apache_beam as beam

class Getting_historical_results(beam.DoFn):
    
    def process(self, element):
        season = element['season'][0]
        element = self.clean_element(season, element)
        for _, row in element.iterrows():
            try:
                row['local_goals'] = int(row['local_goals'])
                row['away_goals'] = int(row['away_goals'])
            except:
                row['result'] = '0–0'
                row['local_goals'] = 0
                row['away_goals'] = 0
            yield row.to_dict()
            
    def clean_element(self, season, element):
        element = element.drop(columns=['season'])
        value_list = ['local']
        value_list.extend(element.iloc[:, 0].tolist())
        element.columns = value_list
        element = element.melt(id_vars=['local'], var_name='away', value_name='result')
        element = element[(element['local'] != element['away'])]
        element[['local_goals', 'away_goals']] = element['result'].str.split('–', n=1, expand=True)
        element['season'] = season
        element = element.astype(str)
        return element

output_gcs = 'gs://trim-heaven-415202/laliga_output/results/historical_results'
with beam.Pipeline(options=PipelineOptions()) as p:
    df = (p 
     | 'Read parquet' >> beam.io.ReadFromParquetBatched(f"gs://trim-heaven-415202/laliga/results/*")
     | 'Convert to pandas' >> beam.Map(lambda table: table.to_pandas())
     | 'Transform Columns' >> beam.ParDo(Getting_historical_results())
     | 'Uploading to GCS' >> beam.io.WriteToParquet(output_gcs, pyarrow.schema(
                                                  [('local', pyarrow.string()), 
                                                   ('away', pyarrow.string()), 
                                                   ('result', pyarrow.string()),
                                                   ('local_goals', pyarrow.int64()),
                                                   ('away_goals', pyarrow.int64()),
                                                   ('season', pyarrow.string())]
     )))