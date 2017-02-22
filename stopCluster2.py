import json, requests

main_url = 'https://ge-digital.cloud.databricks.com/api/2.0/'
dbricks_user = ' '
dbricks_pass = ' '
credentials = (dbricks_user,dbricks_pass)


def createCluster(url,auth):
	url = url + 'clusters/create'
	auth = auth
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

	resp = requests.post(url=url, data=json.dumps(payload), auth=auth)
	respDict = resp.json()
	#print respDict
	clusterID = respDict['cluster_id']
	print 'Cluster ID : \t', clusterID
	return clusterID

# This function returns running Cluster Ids as the list
def runningClustersIds(url, auth):

	url = url+'clusters/list'
	auth = auth
	#print url,'+++++',auth

	#resp = requests.get(url=url,  auth=('mahipal.annam@ge.com','Mahipal123#'))
	resp= requests.get(url=url,  auth=auth)
	#print resp.status_code
	output = resp.json()
	

	# Running Clusters list
	cList = []
	for cluster in output['clusters']:
		if cluster['state'] =='RUNNING':
			print "Runnling Cluster ID : ", cluster['cluster_id']
			cList.append(cluster['cluster_id'])

	return cList



# This is function for printing details of a cluster for 2.0 API
def myprint(d):
	for k, v in d.iteritems():
    		if isinstance(v, dict):
      			myprint(v)
    		else:
      			print "{0} : {1}".format(k, v)

# This fuction returns details for a given clusterId
def getClusterInfo(url, auth, id):
	url = url+'clusters/get'
	auth = auth
	clusterID = id
	payload = {'cluster_id' : clusterID }
	resp= requests.get(url=url, params=payload, auth=auth)
	result = resp.json()
	#print result

	print 'Cluster Name: ', result['cluster_name']
	print 'Cluster Memory: ', result['cluster_memory_mb']
	print 'JDBC Port: ', result['jdbc_port']
	print 'Nodes Type: ', result['node_type_id']
	print 'Spark Version: ', result['spark_version']
	print 'Created by: ', result['creator_user_name']

	#for info in result:
	#	print info,':\t',result[info]

	#myprint(result)

# This function stops a running cluster taking cluster id as the argument
def stopCluster(c):
	url = 'https://ge-digital.cloud.databricks.com/api/2.0/clusters/delete'
	params = {'cluster_id' : p }
	print "Cluster termination started"
	resp = requests.post(url=url, data=json.dumps(params), auth=(' ',' '))
	print resp.status_code



clust_id = runningClustersIds(main_url,credentials)

getClusterInfo(main_url,credentials,clust_id)

"""
if len(clust_id) > 1:
	print "There are more than one cluster running, the script need enhancements"
else:
	print "There is a single running cluster with cluster_id:",clust_id[0]
	#stopCluster(clust_id[0])
"""










