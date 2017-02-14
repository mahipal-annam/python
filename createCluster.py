import json, requests

main_url = 'https://ge-digital.cloud.databricks.com/api/2.0/'
dbricks_user = 'mahipal.annam@ge.com'
dbricks_pass = 'Mahipal123#'
auth = (dbricks_user,dbricks_pass)

payload = {
		  "cluster_name": "GE-Pilot",
		  "spark_version": "2.0.x-scala2.11",
		  "node_type_id": "r3.xlarge",
		
		  "aws_attributes": {
		    "availability": "SPOT",
		    "zone_id": "us-west-2c"
		  },
		  "num_workers": 2
		}

resp = requests.post(main_url+'clusters/create' data=json.dumps(payload), auth=auth)
respDict = resp.json()
print respDict
clusterID = respDict['cluster_id']
print 'Cluster ID : \t', clusterID



		

