import datetime
import apache_beam as beam
from google.cloud import firestore
import pandas as pd
import io
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
import logging


class ReadFile(beam.DoFn):
    def process(self, file_name):
        from apache_beam.io.filesystems import FileSystems
        reader = FileSystems.open(path=file_name)
        df = pd.read_csv(io.StringIO(reader.read().decode('utf-8')), sep='\t', header=None)
        df.columns = ['impression_timestamp_gmt', 'event_timestamp_gmt', 'event_report_timestamp', 'imp_auction_id', 'mm_uuid', 'organization_id', 'organization_name', 'agency_id',
                      'agency_name', 'advertiser_id', 'advertiser_name', 'event_type', 'pixel_id', 'pixel_name', 'pv_pc_flag', 'pv_time_lag', 'pc_time_lag', 'campaign_id',
                      'campaign_name', 'strategy_id', 'strategy_name', 'concept_id', 'concept_name', 'creative_id', 'creative_name', 'exchange_id', 'exchange_name', 'width',
                      'height', 'site_url', 'mm_v1', 'mm_v2', 'mm_s1', 'mm_s2', 'day_of_week', 'week_hour_part', 'mm_creative_size', 'placement_id', 'deal_id', 'country_id',
                      'country', 'region_id', 'region', 'dma_id', 'dma', 'zip_code_id', 'zip_code', 'conn_speed_id', 'conn_speed', 'isp_id',
                      'isp', 'category_id', 'publisher_id', 'site_id', 'watermark', 'fold_position', 'user_frequency', 'browser_id', 'browser', 'os_id', 'os',
                      'browser_language_id', 'week_part', 'day_part', 'day_hour', 'week_part_hour', 'hour_part', 'week_part_hour_part', 'week_hour', 'batch_id',
                      'browser_language', 'empty_int_1', 'homebiz_type_id', 'homebiz_type', 'inventory_type_id', 'inventory_type', 'device_type_id', 'device_type', 'connected_id',
                      'app_id', 'event_subtype', 'city', 'city_code', 'city_code_id', 'supply_source_id', 'ip_address', 'browser_name', 'browser_version', 'os_name', 'os_version',
                      'model_name', 'brand_name', 'form_factor', 'impressions_stream_uuid', 'clicks', 'pc_conversions', 'pv_conversions', 'pc_revenue', 'pv_revenue',
                      'unkownn_additional_col1', 'unkownn_additional_col2']
        df['impression_timestamp_gmt'] = pd.to_datetime(df['impression_timestamp_gmt'])
        df['event_timestamp_gmt'] = pd.to_datetime(df['event_timestamp_gmt'])
        df['event_report_timestamp'] = pd.to_datetime(df['event_report_timestamp'])
        for index, rows in df.iterrows():
            yield rows


class FirestoreWriteDoFn(beam.DoFn):
    MAX_DOCUMENTS = 200

    def __init__(self, project, collection):
        self._project = project
        self._collection = collection

    def start_bundle(self):
        self._mutations = []

    def finish_bundle(self):
        if self._mutations:
            self._flush_batch()

    def process(self, element):
        self._mutations.append(element)
        if len(self._mutations) > self.MAX_DOCUMENTS:
            self._flush_batch()

    def _flush_batch(self):
        db = firestore.Client(project=self._project)
        batch = db.batch()
        collection = db.collection(self._collection)
        for mutation in self._mutations:
            row_key = f"{mutation['advertiser_id']},{mutation['campaign_id']},{mutation['pixel_id']},{mutation['impression_timestamp_gmt']},{mutation['event_report_timestamp']},{mutation['event_timestamp_gmt']},{mutation['mm_uuid']},{mutation['pv_pc_flag']}, {mutation['imp_auction_id']},{mutation['pc_conversions']},{mutation['pv_conversions']}"
            dict_row = mutation.to_dict()
            if len(self._mutations) == 1:
                # autogenerate document_id
                ref = collection.document(row_key)
                ref.set(dict_row)
            else:
                ref = collection.document(row_key)
                ref.set(dict_row)
            batch.commit()
        self._mutations = []


def dataflow(argv=None):
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest='input', default=None, help="Input file to download")
    parser.add_argument('--collection_name', dest='collection_name', default=None, help="collection name for add data to firestore collection")
    parser.add_argument('--project_id', dest='project_id', default=None, help="project id for GCP account")
    parser.add_argument('--service_account', dest='service_account', default=None, help="default service account")
    known_args, pipeline_options = parser.parse_known_args(argv)
    JOB_NAME = 'firestore-upload-{}'.format(datetime.datetime.now().strftime('%Y-%m-%d-%H%M%S'))
    COLLECTION_NAME = known_args.collection_name
    GCP_PROJECT_ID = known_args.project_id
    DEFAULT_SERVICE_ACCOUNT_EMAIL = known_args.service_account

    beam_option = PipelineOptions(
        # region='us-central1',
        region='us-east4',
        runner='DataflowRunner',
        project=GCP_PROJECT_ID,
        job_name=JOB_NAME,
        temp_location='gs://at_datalake_staging/MediaMath/test/beam',
        staging_location='gs://at_datalake_staging/MediaMath/test/beam',
        disk_size_gb=100
    )
    beam_option.view_as(SetupOptions).save_main_session = True
    google_cloud_options = beam_option.view_as(GoogleCloudOptions)
    google_cloud_options.project = GCP_PROJECT_ID
    google_cloud_options.job_name = JOB_NAME
    google_cloud_options.service_account_email = DEFAULT_SERVICE_ACCOUNT_EMAIL

    with beam.Pipeline(options=beam_option) as p:
        (p
         | 'Reading input file' >> beam.Create([known_args.input])
         | "read file from GCS" >> beam.ParDo(ReadFile())
         | 'Write entities into Firestore' >> beam.ParDo(FirestoreWriteDoFn(GCP_PROJECT_ID, COLLECTION_NAME))
         )


if __name__ == '__main__':
    dataflow()
