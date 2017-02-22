import json, requests

main_url = 'https://ge-digital.cloud.databricks.com/api/2.0/'
dbricks_user = ''
dbricks_pass = ''
credentials = (dbricks_user,dbricks_pass)


# This function returns running Cluster Ids as the list
def runningClustersIds():

	url = 'https://ge-digital.cloud.databricks.com/api/2.0/clusters/list'
	print url
	resp = requests.get(url=url,  auth=('',''))
	#resp= requests.get(url=url,  auth=b)
	#print resp.cookies
	#print 'HTTP Headers: \n\n', resp.headers;
	result = resp.json()
	r = resp.json()


	i = 0
	# Running Clusters list
	cList = []
	for a in r['clusters']:
		#print r['clusters'][i]['cluster_id'],':  \t', r['clusters'][i]['state']
	
		if r['clusters'][i]['state'] =='RUNNING':
		#if a['state'] =='RUNNING':
			print "Runnling Cluster ID : ", r['clusters'][i]['cluster_id']
			cList.append(r['clusters'][i]['cluster_id'])
		i = i + 1
	"""
	else:
		print "Terminated or Pending Clusters: ", r['clusters'][i]['cluster_id']

	"""
	print cList
	return cList


# This function stops a running cluster, taking cluster id as the argument
def stopCluster(p):
	url = 'https://ge-digital.cloud.databricks.com/api/2.0/clusters/delete'
	params = {'cluster_id' : p }
	print "Cluster termination started"
	resp = requests.post(url=url, data=json.dumps(params), auth=(' ',' '))
	print resp.status_code



clust_id = runningClustersIds()

"""
if len(clust_id) > 1:
	print "There are more than one cluster running, the script need enhancements"
else:
	print "There is a single running cluster with cluster_id:",clust_id[0]
	#stopCluster(clust_id[0])
"""










