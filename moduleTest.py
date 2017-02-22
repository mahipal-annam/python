
from dbClusterLib import *

main_url = 'https://ge-digital.cloud.databricks.com/api/2.0/'
dbricks_user = ' '
dbricks_pass = ' '
credentials = (dbricks_user,dbricks_pass)



clusterID = runningClustersIds(main_url,credentials)
#print clusterID

if len(clusterID) == 1 :
	getClusterInfo(main_url,credentials,clusterID)
else:
	print "The cluster ID is null OR there are more than one running cluster"

"""
clusID = createCluster(main_url,credentials)
print clusID
"""
