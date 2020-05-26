# DGClusteringGraphX
GraphX implementation of SCAN
# Input data
The input graph must be an adjacency list like the following example: //VertexID:Neighbour1ID,Neighbour2ID,...,NeighbournID.
if we have a list of edges, ADJ class in this project can genere an an adjacency list from a liste of edges.
# Output data
The output result is also a text file, that contains in each line the VertexID and its clusterID, the next example for the output result 
VertexID:ClusterID Note that the -1 and -2 clusterID represent respectively the outliers and the bridges
# Compilation
mvn package
 
