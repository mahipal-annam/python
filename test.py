import json, requests



def myprint(d):
	for k, v in d.iteritems():
    		if isinstance(v, dict):
      			myprint(v)
    		else:
      			print "{0} : {1}".format(k, v)



url = 'https://ge-digital.cloud.databricks.com/api/2.0/clusters/list'
#payload = {'cluster_id' : '0212-042220-waver12'}

resp= requests.get(url=url,  auth=('mahipal.annam@ge.com','Mahipal123#'))
#print resp.cookies
#print 'HTTP Headers: \n\n', resp.headers;
result = resp.json()
r = resp.json()
st= r['clusters'][0]['state']
print st

i = 0
for a in r['clusters']:
	print r['clusters'][i]['cluster_id'],':  \t', r['clusters'][i]['state']
	
	if r['clusters'][i]['state'] =='RUNNING':
		print "Runnling Cluster ID : ", r['clusters'][i]['cluster_id']
	"""
	else:
		print "Terminated or Pending Clusters: ", r['clusters'][i]['cluster_id']

	"""
	i = i + 1








