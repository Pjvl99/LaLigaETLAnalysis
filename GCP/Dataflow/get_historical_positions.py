from apache_beam.options.pipeline_options import PipelineOptions
import numpy as np
import pyarrow
import apache_beam as beam

class Getting_historical_positions(beam.DoFn):
    
    def process(self, element):
        season = element['season'][0]
        element = self.clean_element(season, element)
        for _, row in element.iterrows():
            if '*' in str(row['position']):
                row['position'] = int(row['position'].replace('*', ''))
            else:
                row['position'] = int(row['position'])
            yield row.to_dict()
            
    def clean_element(self, season, element):
        element = element.drop(columns=['season'])
        value_list = ['team']
        value_list.extend(range(1, len(element.columns)))
        element.columns = value_list
        element = element.melt(id_vars=['team'], var_name='fixture', value_name='position')
        element = element[element['team'] != 'Temporada regular']
        element['fixture'] = element['fixture'].astype(int)
        element['position'] = element['position'].fillna(-1)
        element['season'] = season
        return element
    
output_gcs = 'gs://trim-heaven-415202/laliga_output/historical_position/historical_positions_by_team'
with beam.Pipeline(options=PipelineOptions()) as p: 
    df = (p 
     | 'Read parquet' >> beam.io.ReadFromParquetBatched(f"gs://trim-heaven-415202/laliga/historical_position/*")
     | 'Convert to pandas' >> beam.Map(lambda table: table.to_pandas())
     | 'Transform Columns' >> beam.ParDo(Getting_historical_positions())
     | 'Uploading to GCS' >> beam.io.WriteToParquet(output_gcs, pyarrow.schema(
                                                  [('team', pyarrow.string()), 
                                                   ('fixture', pyarrow.int64()), 
                                                   ('position', pyarrow.int64()),
                                                   ('season', pyarrow.string())]
     )))