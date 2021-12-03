import argparse
import logging
import apache_beam as beam
from google.cloud import spanner
from apache_beam.io.gcp.experimental import spannerio
from apache_beam import DoFn, io, ParDo, Pipeline
from apache_beam.options.pipeline_options import PipelineOptions


class ParseEntity(DoFn):
    # def __init__(self):
    #     self.output_path = output_path

    def process(self, line):
        # print('line', line)
        tokens = line.split(',')
        myObj = {}
        myObj['STORE_NO'] = int(tokens[0])
        myObj['COM_CD_y'] = int(tokens[1])
        myObj['CON_UPC_NO_Y'] = int(tokens[2])
        myObj['CAS_UPC_NO'] = int(tokens[3])
        myObj['CAS_DSC_TX'] = str(tokens[4])
        myObj['SHF_ALC_QY'] = str(tokens[5])
        myObj['SHF_NO'] = int(tokens[6])
        myObj['SHF_MIN_QY'] = int(tokens[7])
        myObj['AIL_ORN_CD'] = str(tokens[8])
        myObj['AIL_NO'] = int(tokens[9])
        myObj['AIL_LOC_CD'] = str(tokens[10])
        # print('before returning myObj', myObj)
        # yield myObj
        yield spannerio.WriteMutation.insert(table='df_test_table',
                                             columns=('STORE_NO', 'COM_CD_y', 'CON_UPC_NO_Y', 'CAS_UPC_NO', 'CAS_DSC_TX',
                                                      'SHF_ALC_QY', 'SHF_NO', 'SHF_MIN_QY', 'AIL_ORN_CD', 'AIL_NO', 'AIL_LOC_CD', 't'),
                                             values=[(myObj['STORE_NO'], myObj['COM_CD_y'], myObj['CON_UPC_NO_Y'], myObj['CAS_UPC_NO'], myObj['CAS_DSC_TX'], myObj['SHF_ALC_QY'], myObj['SHF_NO'], myObj['SHF_MIN_QY'], myObj['AIL_ORN_CD'], myObj['AIL_NO'], myObj['AIL_LOC_CD'], spanner.COMMIT_TIMESTAMP)])


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output_topic", help="The Cloud Pub/Sub topic to send to. projects/<PROJECT_ID>/topics/<TOPIC_ID>")
    parser.add_argument("--input_file_pattern",
                        help="Path of the output GCS file including the prefix.")
    parser.add_argument("--spanner_project_id",
                        help="Project ID of spanner instance")
    parser.add_argument("--spanner_instance_id", help="Instance of spanner ")
    parser.add_argument("--spanner_database_id",
                        help="Database of spanner instance")

    known_args, pipeline_args = parser.parse_known_args()

    pipeline_options = PipelineOptions(
        pipeline_args, streaming=True, save_main_session=True)

    with Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | "Read from GCS" >> io.ReadFromText(known_args.input_file_pattern, skip_header_lines=1)
            | 'Reshuffle' >> beam.Reshuffle()
            | 'ParseEntity' >> ParDo(ParseEntity())
            | 'WriteToSpanner' >> spannerio.WriteToSpanner(project_id=known_args.spanner_project_id, instance_id=known_args.spanner_instance_id, database_id=known_args.spanner_database_id, max_number_rows=1)
        )

