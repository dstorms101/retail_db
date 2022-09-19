from http import client
import json
import databricks_client


# Create a client
# if you have configured the databricks cli a token file is created at ~/.databrickscfg.
# if not you can get a token through the UI
db_host = "https://dbc-6607d29e-6adf.cloud.databricks.com"
db_token = "dapi20d40c952144f80829b802377577a70d"
client_url = f"{db_host}/api/2.0"

client = databricks_client.create(client_url)
client.auth_pat_token(db_token)
client.ensure_available()

# Get a list of jobs
jobs_dict = client.get("jobs/list")
print(jobs_dict)
job_id = jobs_dict.get("jobs")[0].get("job_id")  # I only have 1 job in this account
print(job_id)


# Run a job by positing a json string with the required parameters to the API
job_details = {}
job_details["job_id"] = job_id
job_run = client.post("jobs/run-now", json=job_details)
print(job_run)
