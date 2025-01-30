"""This module contains the main process of the robot."""

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
from OpenOrchestrator.database.queues import QueueElement

import os 
import json 
import time

from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext
import pandas as pd


def process(orchestrator_connection: OrchestratorConnection, queue_element: QueueElement | None = None) -> None:
    """Do the primary process of the robot."""
    orchestrator_connection.log_trace("Running process.")
    RobotCredentials = orchestrator_connection.get_credential("Robot365User")
    username = RobotCredentials.username
    password = RobotCredentials.password
    sharepoint_site_base = orchestrator_connection.get_constant("AarhusKommuneSharePoint").value


    # SharePoint site URL
    SHAREPOINT_SITE_URL = f"{sharepoint_site_base}/teams/PlannerPowerBI"

    client = sharepoint_client(username, password, SHAREPOINT_SITE_URL)

    excel_path = download_file_from_sharepoint(client, "Shared Documents/PlannerListe.xlsx")

    planner_df = pd.read_excel(excel_path, sheet_name="PlannerListe")

    os.remove(excel_path)
    # Extract the list of valid PlannerNavn
    valid_planners = set(planner_df["PlannerNavn"].tolist())

    # Step 2: Fetch files from SharePoint
    folder_url = "Shared Documents/PowerBi"
    folder = client.web.get_folder_by_server_relative_url(folder_url)
    files = folder.files.get().execute_query()

    # Step 3: Iterate through the files and delete the ones not in the list
    for file in files:
        file_name_without_extension = file.name.replace(".xlsx", "")
        if file_name_without_extension not in valid_planners:
            print(f"Deleting: {file.name} (ID: {file.unique_id})")
            file.delete_object()
            client.execute_query()
            
            # planner_df = pd.read_excel("PlannerListe.xlsx", sheet_name="PlannerListe")


    # Step 1: Prepare the data for the queue
    data = tuple(
        json.dumps({
            "Name": row["PlannerNavn"],
            "URL": row["URL"],
        }) for _, row in planner_df.iterrows()
    )

    references = tuple(row["PlannerNavn"] for _, row in planner_df.iterrows())

    # Step 2: Call bulk_create_queue_elements
    orchestrator_connection.bulk_create_queue_elements("PlannerRefresh", references=references, data=data)
    
    

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