timeout= 300
# [START run_pubsub_server_setup]
import base64, os, re, proto, time
from google.cloud import documentai_v1beta3 as documentai, bigquery
from google.cloud import storage
import simplejson as json
from datetime import datetime
from flask import Flask, request

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "rajatadminkey.json"


app = Flask(__name__)
# [END run_pubsub_server_setup]

# [START run_pubsub_handler]
@app.route('/', methods=['POST'])
def index():
    envelope = request.get_json()
    if not envelope:
        msg = 'no Pub/Sub message received'
        print(f'error: {msg}')
        return f'Bad Request: {msg}', 400

    if not isinstance(envelope, dict) or 'message' not in envelope:
        msg = 'invalid Pub/Sub message format'
        print(f'error: {msg}')
        return f'Bad Request: {msg}', 400

    pubsub_message = envelope['message']

    name = 'World'
    if isinstance(pubsub_message, dict) and 'data' in pubsub_message:
        name = base64.b64decode(pubsub_message['data']).decode('utf-8').strip()

    print(f'Input message: {name}')
    message = json.loads(name)
    batch_id = validate_message(message, 'batch_id')
    file_name_0 = validate_message(message, 'file_name_0')
    file_name_1 = validate_message(message, 'file_name_1')
    file_name_2 = validate_message(message, 'file_name_2')
    file_name_3 = validate_message(message, 'file_name_3')
    file_name_4 = validate_message(message, 'file_name_4')
    project_id= validate_message(message, 'project_id')
    location=validate_message(message, 'location')
    processor_id=validate_message(message, 'processor_id')
    GCS_INPUT_BUCKET=validate_message(message, 'input_bucket')
    GCS_INPUT_PREFIX=validate_message(message, 'input_prefix')
    GCS_OUTPUT_URI=validate_message(message, 'gcs_output_uri')
    bq_table_id = validate_message(message, 'bq_table_id')

    # Output URI Prefix generated here..
    GCS_OUTPUT_URI_PREFIX= str(int(time.time()))+"_"+str(batch_id)

    print("Message received. Batch_id:{}, file0:{}, file1:{}, file2:{}, file3:{}, file4:{}".format(batch_id,file_name_0,file_name_1,file_name_2,file_name_3,file_name_4))
    print("Starting processing:{}".format(datetime.now()))
    client = documentai.DocumentProcessorServiceClient()
    storage_client = storage.Client()
    
    # Sample invoices are stored in gs://cloud-samples-data/documentai/async_invoices/
    #blobs = storage_client.list_blobs(GCS_INPUT_BUCKET, prefix=GCS_INPUT_PREFIX)
    input_configs = []
    #print("Input Files:")
    #for blob in blobs:
    #    if ".pdf" in blob.name:
    #        source = "gs://{bucket}/{name}".format(bucket = GCS_INPUT_BUCKET, name = blob.name)
    #        print(source)
    
    if(file_name_0 != "not_found"):
        input_config_1 = documentai.types.document_processor_service.BatchProcessRequest.BatchInputConfig(
                gcs_source="gs://"+GCS_INPUT_BUCKET+"/"+GCS_INPUT_PREFIX+file_name_0, mime_type="application/pdf")
        input_configs.append(input_config_1)
    if(file_name_1 != "not_found"):
        input_config_2 = documentai.types.document_processor_service.BatchProcessRequest.BatchInputConfig(
                gcs_source="gs://"+GCS_INPUT_BUCKET+"/"+GCS_INPUT_PREFIX+file_name_1, mime_type="application/pdf")
        input_configs.append(input_config_2)
    if(file_name_2 != "not_found"):
        input_config_3 = documentai.types.document_processor_service.BatchProcessRequest.BatchInputConfig(
                gcs_source="gs://"+GCS_INPUT_BUCKET+"/"+GCS_INPUT_PREFIX+file_name_2, mime_type="application/pdf")
        input_configs.append(input_config_3)
    if(file_name_3 != "not_found"):
        input_config_4 = documentai.types.document_processor_service.BatchProcessRequest.BatchInputConfig(
                gcs_source="gs://"+GCS_INPUT_BUCKET+"/"+GCS_INPUT_PREFIX+file_name_3, mime_type="application/pdf")
        input_configs.append(input_config_4)
    if(file_name_4 != "not_found"):
        input_config_5 = documentai.types.document_processor_service.BatchProcessRequest.BatchInputConfig(
                gcs_source="gs://"+GCS_INPUT_BUCKET+"/"+GCS_INPUT_PREFIX+file_name_4, mime_type="application/pdf")
        input_configs.append(input_config_5)

    destination_uri = f"{GCS_OUTPUT_URI}/{GCS_OUTPUT_URI_PREFIX}/"

    # Where to write results
    output_config = documentai.types.document_processor_service.BatchProcessRequest.BatchOutputConfig(
        gcs_destination=destination_uri
    )

    name = f"projects/{project_id}/locations/{location}/processors/{processor_id}"
    docai_request = documentai.types.document_processor_service.BatchProcessRequest(
        name=name,
        input_configs=input_configs,
        output_config=output_config,
    )
    operation = client.batch_process_documents(docai_request)
    # Wait for the operation to finish
    operation.result(timeout=timeout)
    print("Got Async response:{}".format(datetime.now()))

    # Results are written to GCS. Use a regex to find output files
    match = re.match(r"gs://([^/]+)/(.+)", destination_uri)
    output_bucket = match.group(1)
    prefix = match.group(2)

    bucket = storage_client.get_bucket(output_bucket)
    blob_list = list(bucket.list_blobs(prefix=prefix))

    for i, blob in enumerate(blob_list):
        # If JSON file, download the contents of this blob as a bytes object.
        if ".json" in blob.name:
            blob_as_bytes = blob.download_as_string()
            print("downloaded")

            document = documentai.types.Document.from_json(blob_as_bytes)
            print(f"Fetched file {i + 1}")

            entityDict={}
            lineItem_text=""

            for entity in document.entities:
                entity_type = entity.type_
                if(not isEntityInList(entity_type)):
                    continue;
                #print ("Entity Type:{}, Normalized Value:{}, Confidence:{}".format(entity.type_, entity.normalized_value.text, entity.confidence))
                if(entity.normalized_value.text!=""):
                    entity_text = entity.normalized_value.text
                else:
                    entity_text = re.sub('[":\""]', '',entity.mention_text)
        
                # Placeholder code below to test whether the amount fields have strings with commas coming in. Converting them to floats for now.        
                if("amount" in entity_type and entity.normalized_value.text ==''):
                    entity_text = float(re.sub('\D', '', entity.mention_text))

                if(entity_type=="line_item"):
                    lineItem_text =  lineItem_text+'{'
                    currentLIKeys=""
                    for prop in entity.properties:                
                        pName=prop.type_[prop.type_.index("/")+1:]
                        pName=getLineItemKeyName(currentLIKeys, pName)
                        if("skip" not in pName):
                            if("amount" in pName and prop.normalized_value.text ==''):   
                                print("prop.normalized_value.text: {}".format(prop.normalized_value.text))
                                prop.mention_text = float(re.sub('\D', '', prop.mention_text))
                            elif(prop.normalized_value.text!=""):
                                prop.mention_text =re.sub('["\""]', '',prop.normalized_value.text)
                            # Making sure there are no special characters in the lineItem text
                            prop.mention_text= re.sub('[":\""]', '',prop.mention_text)
                            lineItem_text = lineItem_text+ "\""+pName +"\""+":"+ "\""+prop.mention_text+ "\""+","   
                            currentLIKeys=currentLIKeys+pName
                    lineItem_text = lineItem_text[0:lineItem_text.rindex(",")]
            
                    lineItem_text = lineItem_text+'},'
                    #print("lineItem_text text before:{}".format(lineItem_text))
                if(entity_type!="line_item"):
                    entityDict[entity_type]=entity_text    
    
            if(lineItem_text!=""):
                lineItem_text = lineItem_text[0:lineItem_text.rindex(",")]
                lineItem_text = "["+lineItem_text+"]"     
                #Take out any special characters
                lineItem_text = lineItem_text.replace('\n', '')
                #print("Line Item Text: -- {}".format(lineItem_text))
                lineItem_t = json.loads(lineItem_text)
    
                #print("Final Line Item:{}".format(lineItem_t))
                entityDict["line_item"]=lineItem_t    
    
            entityDict["file_name"]=blob.name
            #Calling the WiteToBQ Method
            writeToBQ(entityDict,bq_table_id) 
    
    print ("Done processing the files")
    return ('', 204)
# [END run_pubsub_handler]

# Write to BQ Method
def writeToBQ(documentEntities: dict, bq_table_id:str):
    print("Inserting into BQ ************** ")
    #Insert into BQ    
    client = bigquery.Client()    
    table_id = bq_table_id     
    table = client.get_table(table_id)

    print ('Adding the row')
    rows_to_insert= [documentEntities]

    print (' ********** NEW Row Column: ',rows_to_insert)
    errors = client.insert_rows_json(table, rows_to_insert) 
    if errors == []:
        print("New rows have been added.") 
    else:
        print ('Encountered errors: ',errors)

def download_blob(bucket_name, source_blob_name):
    """Downloads a blob from the bucket."""

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    return blob

def getLineItemKeyName(lineItem, key):
    #print(lineItem)
    if(key+"3" in lineItem):
        return "skip"
    elif(key+"2" in lineItem):
        return key+"3"
    elif(key in lineItem):
        return key+"2"
    else:
        return key
    
def isEntityInList(entityType:str):
    if(entityType in ('carrier','currency','currency_exchange_rate','customer_tax_id',
        'delivery_date','due_date','freight_amount','invoice_date','invoice_id''net_amount',
        'payment_terms','purchase_order','receiver_address','receiver_email','receiver_name',
        'receiver_phone','remit_to_address','remit_to_name','ship_from_address',
        'ship_from_name','ship_to_address','ship_to_name','supplier_address','supplier_email',
        'supplier_name','supplier_phone','supplier_registration','supplier_tax_id',
        'supplier_website','total_amount')):
        return True;
    else:
        return False;

# [START message_validatation_helper]
def validate_message(message, param):
    var = message.get(param)
    
    if not var:
        #raise ValueError('{} is not provided. Make sure you have \
        #                  property {} in the request'.format(param, param))
        var = "not_found"
    return var

if __name__ == '__main__':
    PORT = int(os.getenv('PORT')) if os.getenv('PORT') else 8080

    # This is used when running locally. Gunicorn is used to run the
    # application on Cloud Run. See entrypoint in Dockerfile.
    app.run(host='127.0.0.1', port=PORT, debug=True)
