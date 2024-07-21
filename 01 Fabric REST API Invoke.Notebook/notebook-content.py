# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "91cac876-080b-4b86-ae2d-47506199aa8b",
# META       "default_lakehouse_name": "wwilakehouse",
# META       "default_lakehouse_workspace_id": "01fc90bd-e289-4438-84bf-130c0b13882d",
# META       "known_lakehouses": [
# META         {
# META           "id": "91cac876-080b-4b86-ae2d-47506199aa8b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

print("Hello")

# CELL ********************

import requests
import json
from requests.adapters import HTTPAdapter, Retry
from notebookutils import mssparkutils

# CELL ********************

def invoke_fabric_api_request(method, uri, payload=None):
    
    API_ENDPOINT = "api.fabric.microsoft.com/v1"

    headers = {
        "Authorization": "Bearer " + mssparkutils.credentials.getToken("pbi"),
        "Content-Type": "application/json"
    }

    try:
        url = f"https://{API_ENDPOINT}/{uri}"
            
        session = requests.Session()
        retries = Retry(total=3, backoff_factor=5, status_forcelist=[502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries)
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        response = session.request(method, url, headers=headers, json=payload, timeout=240)

        response_details = {
            'status_code': response.status_code,
            'response': response.json() if response.content else {},
            'headers': dict(response.headers) 
        }
        print(json.dumps(response_details, indent=2))

    except requests.RequestException as ex:
        print(ex)

# CELL ********************

# Set up parameters
workspace_id = mssparkutils.runtime.context['currentWorkspaceId']
item_type = "DataPipeline"

# CELL ********************

print(workspace_id)

# MARKDOWN ********************

# ## create data pipeline

# CELL ********************

method = "post"
uri = f"workspaces/{workspace_id}/items"

payload = {
  "displayName": "pip_rest_api_1",
  "type": item_type,
  "description": "fabric rest api 101"
}

invoke_fabric_api_request(method, uri, payload)

# CELL ********************

# with item definition (payload)
method = "post"
uri = f"workspaces/{workspace_id}/items"

payload = {
  "displayName": "pip_rest_api_2",
  "type": item_type,
  "definition": {
    "parts": [
      {
        "path": "pipeline-content.json",
        "payload": "ewogICAgIm5hbWUiOiAicGlwZWxpbmUxIiwKICAgICJvYmplY3RJZCI6ICI0YWRlZjRlMC1hYWU5LTQ2ZmEtOTE1My0xYTdhNjBkMzJiMzkiLAogICAgInByb3BlcnRpZXMiOiB7CiAgICAgICAgImFjdGl2aXRpZXMiOiBbXSwKICAgICAgICAiYW5ub3RhdGlvbnMiOiBbXSwKICAgICAgICAibGFzdE1vZGlmaWVkQnlPYmplY3RJZCI6ICIzOTVkNDcyMi1lMDM0LTRmZTUtYTk1Yy01MjVkNWIwYWRlZWIiLAogICAgICAgICJsYXN0UHVibGlzaFRpbWUiOiAiMjAyNC0wNC0xMFQxMTozNTo1NFoiCiAgICB9Cn0=",
        "payloadType": "InlineBase64"
      }
    ]
  }
}

invoke_fabric_api_request(method, uri, payload)

# MARKDOWN ********************

# ### delete pipelines

# CELL ********************

item_id = ""
method = "delete"
uri = f"workspaces/{workspace_id}/items/{item_id}"

invoke_fabric_api_request(method, uri)

# MARKDOWN ********************

# ### list pipelines

# CELL ********************

# list pipelines
method = "get"
uri = f"workspaces/{workspace_id}/items?type={item_type}"

invoke_fabric_api_request(method, uri)

# MARKDOWN ********************

# ### get data pipeline

# CELL ********************


# get metadata
item_id = ""
method = "get"
uri = f"workspaces/{workspace_id}/items/{item_id}"

invoke_fabric_api_request(method, uri)

# MARKDOWN ********************

# ### run pipeline

# CELL ********************

item_id = ""
job_type = "Pipeline"
method = "post"
uri = f"workspaces/{workspace_id}/items/{item_id}/jobs/instances?jobType={job_type}"

payload = {
   "executionData": {
        "parameters": {
            "param1": "101"
        }
    }
}

invoke_fabric_api_request(method, uri, payload)
