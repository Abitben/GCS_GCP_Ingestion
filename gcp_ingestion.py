# used to authenticate the service account

from colorama import Fore, Style

from google.oauth2 import service_account
# used to interact with GCS
from google.cloud import storage

import requests
# used to stream the data, not store it in memory
from io import BytesIO
import io

# used to read the data
import pandas as pd
# used to read and extract the data from the zip file
import zipfile
import gzip

class FromFileToGCS:
    """
    A class used to process data from a URL and upload it to Google Cloud Storage (GCS)

    ...

    Attributes
    ----------
    bucket_name : str
        a formatted string to print out the name of the GCS bucket
    credentials_path : str
        a formatted string that holds the path of the service account credentials file
    url : str
        a formatted string that holds the URL of the data to be processed
    credentials : google.auth.credentials.Credentials
        service account credentials, which are used for authentication
    storage_client : google.cloud.storage.client.Client
        the client for interacting with the GCS API

    Methods
    -------
    create_bucket():
        Creates a new bucket in GCS
    download_and_upload_from_URL(url, destination_blob_name):
        Downloads data from a URL and uploads it to GCS
    local_to_gcs(file_path, destination_blob_name):
        Uploads a local file to GCS
    list_blobs():
        Extracts data from a compressed file and uploads it to GCS
    extract_and_upload_sel(blobs):
        Extracts data from a compressed file and uploads it to GCS
    """

    def __init__(self, bucket_name, credentials_path):
        """
        Constructs all the necessary attributes for the DataProcessor object.

        Parameters
        ----------
            bucket_name : str
                name of the GCS bucket
            credentials_path : str
                path of the service account credentials file
        """
        self.bucket_name = bucket_name
        self.credentials = service_account.Credentials.from_service_account_file(credentials_path)
        self.storage_client = storage.Client(credentials=self.credentials)

    def create_bucket(self):
        """
        Creates a new bucket in GCS. If the bucket already exists, it prints a message and does nothing.
        """

        try:
            self.storage_client.get_bucket(self.bucket_name).exists()
            print("Bucket already exists.")
        except:
            bucket = self.storage_client.bucket(self.bucket_name)
            # set the storage class to COLDLINE, which is the cheapest one
            bucket.storage_class = "COLDLINE"
            # set the location to europe-west1, which is in the EU
            new_bucket = self.storage_client.create_bucket(bucket, location="europe-west1")
            print('A new bucket created at {}'.format(new_bucket.name))

    def download_and_upload_from_URL(self, url, destination_blob_name):
        """
        Downloads data from a URL and uploads it to GCS.

        Parameters
        ----------
            url : str
                URL of the data to be downloaded
            destination_blob_name : str
                name of the blob where the data will be uploaded in GCS
        """

        self.url = url
        self.destination_blob_name = destination_blob_name
        self.destination_blob_name_raw = 'raw_format/' + destination_blob_name
        response = requests.get(self.url)
        if response.status_code == 200:
            file_stream = BytesIO(response.content)
            bucket = self.storage_client.bucket(self.bucket_name)
            blob = bucket.blob(self.destination_blob_name_raw)
            blob.upload_from_file(file_stream, content_type='application/zip')
            print(f"{Fore.GREEN}Raw file {self.destination_blob_name} downloaded and uploaded to GCS successfully to {self.destination_blob_name_raw}.{Style.RESET_ALL}")
        else:
            print(f"{Fore.RED}Request failed with error {response.status_code}.{Style.RESET_ALL}")

    def local_to_gcs(self, file_path, destination_blob_name):
        """
        Uploads a local file to GCS.

        Parameters
        ----------
            file_path : str
                path of the local file to be uploaded
            destination_blob_name : str
                name of the blob where the data will be uploaded in GCS
        """

        self.file_path = file_path
        self.destination_blob_name = destination_blob_name
        self.destination_blob_name_raw = 'raw_format/' + destination_blob_name
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(self.destination_blob_name_raw)
        blob.upload_from_filename(self.file_path)
        print(f"{Fore.GREEN}Raw file {self.destination_blob_name} uploaded to GCS successfully to {self.destination_blob_name_raw}.{Style.RESET_ALL}")

    def list_blobs(self):
        """
        Extracts data from a compressed file and uploads it to GCS.
        """
        prefix = f'raw_format/'
        bucket = self.storage_client.get_bucket(self.bucket_name)
        print(bucket.name)
        blobs = list(self.storage_client.list_blobs(self.bucket_name, prefix=prefix))
        return blobs

    def extract_and_upload_sel(self, blobs):
        """
        Extracts data from a compressed file and uploads it to GCS.
        Parameters
        ----------
            blobs : list
                list of blobs to be extracted and uploaded to GCS
        """
        self.blobs = blobs
        bucket = self.storage_client.get_bucket(self.bucket_name)
        for blob in blobs:
            print(f"{Fore.CYAN}Start extracting and uploading to GCS : {blob.name}{Style.RESET_ALL}")
            zip_data = blob.download_as_bytes()

            if self.destination_blob_name.endswith('.zip'):
                with zipfile.ZipFile(io.BytesIO(zip_data)) as z:
                    for file_name in z.namelist():
                        with z.open(file_name) as f:
                            data = pd.read_csv(f)
                            file_name_clean = file_name.replace(".zip", "")
                            destination_blob_name = f'raw_csv/{file_name_clean}.csv'
                            csv_data = data.to_csv(index=False)
                            blob_output = bucket.blob(destination_blob_name)
                            blob_output.upload_from_string(csv_data, content_type='text/csv')
                            print(f"{Fore.GREEN}{self.destination_blob_name_raw} is uncompressed and uploaded to {destination_blob_name}.{Style.RESET_ALL}")

            elif self.destination_blob_name.endswith('.gz'):
                with gzip.GzipFile(fileobj=io.BytesIO(zip_data)) as f:
                    data = pd.read_csv(f)
                    file_name_clean = self.destination_blob_name.replace(".gz", "")
                    destination_blob_name = f'raw_csv/{file_name_clean}.csv'
                    csv_data = data.to_csv(index=False)
                    blob_output = bucket.blob(destination_blob_name)
                    blob_output.upload_from_string(csv_data, content_type='text/csv')
                    print(f"{Fore.GREEN}{self.destination_blob_name_raw} is uncompressed and uploaded to {destination_blob_name}.{Style.RESET_ALL}")

            elif self.destination_blob_name.endswith('.csv'):
                with io.BytesIO(zip_data) as f:
                    data = pd.read_csv(f)
                    destination_blob_name = f'raw_csv/{destination_blob_name}'
                    csv_data = data.to_csv(index=False)
                    blob_output = bucket.blob(destination_blob_name)
                    blob_output.upload_from_string(csv_data, content_type='text/csv')
                    print(f"{Fore.GREEN}{self.destination_blob_name_raw} is uncompressed and uploaded to {destination_blob_name}.{Style.RESET_ALL}")



# from pandas.io to google biquery
import pandas_gbq
from google.cloud import bigquery
from io import StringIO


class FromGCStoGBQ:
    """
    A class used to process data from Google Cloud Storage (GCS) and BigQuery (BQ)

    ...

    Attributes
    ----------
    credentials : google.auth.credentials.Credentials
        Google Cloud credentials from a service account file
    bq_client : google.cloud.bigquery.client.Client
        A BigQuery client object
    storage_client : google.cloud.storage.client.Client
        A Cloud Storage client object
    project_id : str
        Google Cloud project ID
    dataset_name : str
        BigQuery dataset name
    bucket_name : str
        Cloud Storage bucket name

    Methods
    -------
    create_dataset():
        Creates a new BigQuery dataset
    list_blobs():
        Lists all blobs in the Cloud Storage bucket
    upload_to_bq(blobs):
        Uploads blobs data to BigQuery
    """

    def __init__(self, credentials_path, project_id, dataset_name, bucket_name):
        """
        Constructs all the necessary attributes for the GCS_BQ_Processor object.

        Parameters
        ----------
            credentials_path : str
                Path to the service account file
            project_id : str
                Google Cloud project ID
            dataset_name : str
                BigQuery dataset name
            bucket_name : str
                Cloud Storage bucket name
        """
        self.credentials = service_account.Credentials.from_service_account_file(credentials_path)
        self.bq_client = bigquery.Client(credentials=self.credentials, project=project_id)
        self.storage_client = storage.Client(credentials=self.credentials)
        self.project_id = project_id
        self.dataset_name = dataset_name
        self.bucket_name = bucket_name
    

    def create_dataset(self):
        """
        Creates a new BigQuery dataset
        """
        dataset_id = f"{self.project_id}.{self.dataset_name}"
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "EU"
        self.bq_client.create_dataset(dataset, timeout=30, exists_ok=True)
        print(f"{Fore.GREEN}Created dataset (or already exists) {self.bq_client.project}.{dataset.dataset_id}{Style.RESET_ALL}")

    def list_blobs(self):
        """
        Lists all blobs in the Cloud Storage bucket

        Returns
        -------
        list
            a list of blobs in the bucket
        """
        self.prefix = f'raw_csv/'
        bucket = self.storage_client.get_bucket(self.bucket_name)
        print(bucket.name)
        blobs = list(self.storage_client.list_blobs(self.bucket_name, prefix=self.prefix))
        return blobs

    def upload_to_bq(self, blobs):
        """
        Uploads blobs data to BigQuery

        Parameters
        ----------
        blobs : list
            a list of blobs to be uploaded
        """
        for blob in blobs:
            blob_data = blob.download_as_text()
            df = pd.read_csv(StringIO(blob_data))
            print(f"{Fore.BLUE}{blob.name}{Style.RESET_ALL}")
            blob_name = blob.name.replace(f"raw_csv/", "").replace(".csv", "")
            table_name = self.project_id + '.' + "raw_data" + "." + blob_name
            pandas_gbq.to_gbq(df, table_name, project_id=self.project_id, if_exists='replace', api_method= "load_csv")
            print(f"{Fore.GREEN}{blob.name} is uploaded to {table_name}{Style.RESET_ALL}")

