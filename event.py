

from googleapiclient.discovery import build
import os
import logging


def conversions_data_pipelines(event, context):
    logging.getLogger().setLevel(logging.INFO)
    if 'conversion' in event['name']:
        file_name = event['name']
        bucket_name = event['bucket']
        path = f'gs://{bucket_name}/{file_name}'
        COLLECTION_NAME = os.environ.get('CONVERSIONS_REPORT_COLLECTION_NAME')
        GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
        DEFAULT_SERVICE_ACCOUNT_EMAIL = os.environ.get('DEFAULT_SERVICE_ACCOUNT_EMAIL')
        # cache_discovery should be set to False to avoid errors
        dataflow = build('dataflow', 'v1b3', cache_discovery=False)
        params_dict = {
            "input": path,
            "collection_name": COLLECTION_NAME,
            "project_id": GCP_PROJECT_ID,
            "service_account": DEFAULT_SERVICE_ACCOUNT_EMAIL
        }
        request = dataflow.projects().locations().flexTemplates().launch(
            projectId=GCP_PROJECT_ID,
            location='us-east4',
            body={
                'launch_parameter': {
                    'jobName': 'firestore-upload-`date +%Y-%m-%d-%H%M%S`',
                    'parameters': params_dict,
                    'containerSpecGcsPath': 'gs://dataflow_firestore/samples/dataflow/templates/streaming-beam.json',
                }
            }
        )

        request.execute()

    else:
        logging.info("not conversions file")
