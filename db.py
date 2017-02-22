import json, requests

"""
url = 'https://ge-digital.cloud.databricks.com/api/2.0/clusters/get'
payload = {'cluster_id' : '0212-042220-waver12'}

resp= requests.get(url=url, params=payload, auth=(' ',' '))
#print resp.cookies
#print 'HTTP Headers: \n\n', resp.headers;
result = resp.json()
#print 'HTTP Response JSON : \n\n', result;
#print type(result)
"""

# This syntax if for 2.0 API

def myprint(d):
	for k, v in d.iteritems():
    		if isinstance(v, dict):
      			myprint(v)
    		else:
      			print "{0} : {1}".format(k, v)


#myprint(result)

def getCluster(id):
	url = 'https://ge-digital.cloud.databricks.com/api/2.0/clusters/get'
	clusterID = id
	payload = {'cluster_id' : clusterID }
	resp= requests.get(url=url, params=payload, auth=('mahipal.annam@ge.com','Mahipal123#'))
	#print resp.cookies
	#print 'HTTP Headers: \n\n', resp.headers;
	result = resp.json()
	myprint(result)


def createCluster():
	url = 'https://ge-digital.cloud.databricks.com/api/2.0/clusters/create'
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

	resp = requests.post(url=url, data=json.dumps(payload), auth=('mahipal.annam@ge.com','Mahipal123#'))
	respDict = resp.json()
	print respDict
	clusterID = respDict['cluster_id']
	print 'Cluster ID : \t', clusterID


def stopCluster(p):
	url = 'https://ge-digital.cloud.databricks.com/api/2.0/clusters/delete'
	params = p
	print "Cluster termination started"
	resp = requests.post(url=url, data=json.dumps(params), auth=('mahipal.annam@ge.com','Mahipal123#'))
	print resp
	



if result['state'] == "RUNNING":
	print "Cluster is Up and Running"
	print result['cluster_id']
	stopCluster(payload)
else:
	createCluster()




# 1.2 version API
"""

for r in result:
	if r['status'] == 'Running':
		print '===================='
		print 'Cluster Name: ',r['name'], '\n' 'Cluster ID:' , r['id'],'\n' 'Number of Workers', r['numWorkers'],'\n','Driver IP: ', r['driverIp']

		print '========'
		ClustID = r['id']

print '====',ClustID

"""



		

