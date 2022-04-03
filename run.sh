export TEMPLATE_PATH="gs://dataflow_firestore/samples/dataflow/templates/streaming-beam.json"
export TEMPLATE_IMAGE="gcr.io/at-engineering-staging/samples/dataflow/streaming-beam:latest"

# for build and push image to GCR
gcloud builds submit --tag "$TEMPLATE_IMAGE" .

# just for local run test 
gcloud dataflow flex-template build "gs://dataflow_firestore/samples/dataflow/templates/streaming-beam.json" --image "gcr.io/at-engineering-staging/samples/dataflow/streaming-beam:latest" --sdk-language "PYTHON" --metadata-file "metadata.json"

gcloud dataflow flex-template run "firestore-upload-`date +%Y-%m-%d-%H%M%S`" \
    --template-file-gcs-location $TEMPLATE_PATH \
    --parameters input="gs://at_datalake_staging/MediaMath/log-level/attributed-events/2021/04/24/conversion/mm_attributed_events_102130_20210424_conversion_2021042500.txt"\
    --parameters collection_name="mediamath_loglevel_conversions"\
    --parameters project_id="at-engineering-staging"\
    --parameters service_account="363220685993-compute@developer.gserviceaccount.com"\
    --region "us-east4"\
    --additional-experiments use_runner_v2
