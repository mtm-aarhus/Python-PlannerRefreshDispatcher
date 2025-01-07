from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection

import os 
import json 
import time

from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext
import pandas as pd

# SharePoint site URL
def sharepoint_client(username: str, password: str, sharepoint_site_url: str) -> ClientContext:
    """
    Creates and returns a SharePoint client context.
    """
    # Authenticate to SharePoint
    ctx = ClientContext(sharepoint_site_url).with_credentials(UserCredential(username, password))

    # Load and verify connection
    web = ctx.web
    ctx.load(web)
    ctx.execute_query()

    print(f"Authenticated successfully. Site Title: {web.properties['Title']}")
    return ctx

def download_file_from_sharepoint(client: ClientContext, sharepoint_file_url: str) -> str:
    """
    Downloads a file from SharePoint and returns the local file path.
    Handles both cases where subfolders exist or only the root folder is used.
    """
    # Extract the root folder, folder path, and file name
    path_parts = sharepoint_file_url.split('/')
    DOCUMENT_LIBRARY = path_parts[0]  # Root folder name (document library)
    FOLDER_PATH = '/'.join(path_parts[1:-1]) if len(path_parts) > 2 else ''  # Subfolders inside root, or empty if none
    file_name = path_parts[-1]  # File name

    # Construct the local folder path inside the Documents folder
    documents_folder = os.path.join(os.path.expanduser("~"), "Documents", FOLDER_PATH) if FOLDER_PATH else os.path.join(os.path.expanduser("~"), "Documents", DOCUMENT_LIBRARY)

    # Ensure the folder exists
    if not os.path.exists(documents_folder):
        os.makedirs(documents_folder)

    # Define the download path inside the folder
    download_path = os.path.join(os.getcwd(), file_name)

    # Download the file from SharePoint
    with open(download_path, "wb") as local_file:
        file = (
            client.web.get_file_by_server_relative_path(sharepoint_file_url)
            .download(local_file)
            .execute_query()
        )
    # Define the maximum wait time (60 seconds) and check interval (1 second)
    wait_time = 60  # 60 seconds
    elapsed_time = 0
    check_interval = 1  # Check every 1 second


    # While loop to check if the file exists at `file_path`
    while not os.path.exists(download_path) and elapsed_time < wait_time:
        time.sleep(check_interval)  # Wait 1 second
        elapsed_time += check_interval

    # After the loop, check if the file still doesn't exist and raise an error
    if not os.path.exists(download_path):
        raise FileNotFoundError(f"File not found at {download_path} after waiting for {wait_time} seconds.")

    print(f"[Ok] file has been downloaded into: {download_path}")
    return download_path

orchestrator_connection = OrchestratorConnection("ExcelRefresher", os.getenv('OpenOrchestratorSQL'), os.getenv('OpenOrchestratorKey'), None)
