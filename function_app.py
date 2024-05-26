import azure.functions as func
import logging
from PIL import Image
from azure.storage.blob import BlobServiceClient
import io
import os


app = func.FunctionApp()

@app.function_name(name="BlobTrigger")
@app.blob_trigger(arg_name="myblob", path="gallery",
                               connection="AzureWebJobsStorage") 
def blob_trigger(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}")
    try:
        conn_string=os.environ['AzureWebJobsStorage']
        blob_service_client = BlobServiceClient.from_connection_string(conn_string)
        blob_name = myblob.name.split("/")[-1]
        container_name = "gallery"
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        
        blob_properties = blob_client.get_blob_properties()
        metadata = blob_properties.metadata
        if 'processed' in metadata and metadata['processed'] == 'true':
            logging.info("Blob has been processed already. Skipping.")
            return

        data = blob_client.download_blob().readall()
        image = Image.open(io.BytesIO(data))

        logging.info(f"OG size: {image.size}")
        resized_image = image.resize((128, 128))
        image_bytes = io.BytesIO()

        resized_image.save(image_bytes, format=image.format)   
        image_bytes = image_bytes.getvalue()   
        new_image = Image.open(io.BytesIO(image_bytes))
        logging.info(f"New size: {new_image.size}")
        new_name = "resized-" + blob_name
        new_blob_client = blob_service_client.get_blob_client(container=container_name, blob=new_name)
        new_blob_client.upload_blob(image_bytes, overwrite=True, metadata={'processed': 'true'})

        logging.info(f"Python blob trigger function executed successfully."
                    f"Name: {myblob.name}")
    except Exception as e:
        logging.error(f"Error: {e}")