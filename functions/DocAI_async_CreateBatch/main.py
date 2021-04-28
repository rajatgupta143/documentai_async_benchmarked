from google.cloud import pubsub_v1
from google.cloud import storage
import os
from flask import escape
import simplejson as json

def invoke_batch_process(request):

    project_id = os.environ['project_id']
    bq_topic_name= os.environ['bq_topic_name']
    input_bucket= os.environ['input_bucket_name']
    input_prefix= os.environ['input_prefix']
    processor_id= os.environ['processor_id']
    gcs_output_uri= os.environ['GCS_OUTPUT_URI']
    bq_table_id= os.environ['bq_table_id']
    location= os.environ['location']

    # Publisher information
    publisher = pubsub_v1.PublisherClient()
    print('Topic Name: {}'.format(bq_topic_name))
    topic_path = publisher.topic_path(project_id, bq_topic_name)
    print('Topic Path: {}'.format(topic_path))

    #Read all the files
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(input_bucket)
    blob_list = list(bucket.list_blobs(prefix=input_prefix))
    print("Input files:")
    
    batch_counter=0
    file_counter=0
    futures = []
    batch_msg={}
    for i, blob in enumerate(blob_list):
        #print(blob.name)
        #print("I:{}, file_counter:{}, batch_counter:{}".format(i,file_counter, batch_counter))
        batch_msg["batch_id"]=batch_counter
        batch_msg["bq_table_id"]=bq_table_id
        batch_msg["processor_id"]=processor_id
        batch_msg["gcs_output_uri"]=gcs_output_uri
        batch_msg["input_bucket"]=input_bucket
        batch_msg["project_id"]=project_id
        batch_msg["location"]=location
        batch_msg["input_prefix"]=input_prefix+"/"
        if ".pdf" in blob.name:            
            file_id = "file_name_"+str(file_counter)
            batch_msg[file_id]= blob.name[blob.name.find('/')+1:]
            file_counter = file_counter+1
        if (file_counter==5):
            file_counter=0;
            batch_counter=batch_counter+1
            print("Batch message:{}".format(batch_msg))
            
            message_data = json.dumps(batch_msg).encode('utf-8')
            future = publisher.publish(topic_path, data=message_data)
            futures.append(future)
            for future in futures:
                future.result()
            print ("Sent to PubSub")
            batch_msg={}

        if(i+1==len(blob_list)): #Last batch
            print("Batch message:{}".format(batch_msg))
            message_data = json.dumps(batch_msg).encode('utf-8')
            future = publisher.publish(topic_path, data=message_data)
            futures.append(future)
            for future in futures:
                future.result()
            print ("Sent to PubSub")
            batch_msg={}
    return "Done!"
