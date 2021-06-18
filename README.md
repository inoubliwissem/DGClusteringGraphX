# DGClusteringGraphX
In this project we propose an implementaion of the structral graph clustering using the GraphX framework which based on apache spark.
@author Wissem Inoubli <inoubliwissem@gmail.com>

## Required software and libraries
-Java 8
-SBT
Spark 2.4 or higher
## R##un
1. Start YARN, HDFS and Spark history server
  ` dfs-start.sh`
  `start-yarn.sh`
2. Push the input graph into HDFS
   ```shell
      $ hdfs dfs -put inputgraph (list of edges) /exp/
   ```
3. Build the jar file
      ```shell
      $ mvn package
   ```
# Input data
The input graph must be an adjacency list like the following example:
//VertexID:Neighbour1ID,Neighbour2ID,...,NeighbournID.
if we have a list of edges, ADJ class in this project can convert  edge lits to an adjacency list.

      ```shell
      $ spark-submit --deploy-mode cluster --num-executors 3 --executor-cores 2 --executor-memory 8GB --class Adj JarFile hdfs://master:9000/exp/inputgraph hdfs://master:9000/exp/pathToRstlts
   ```
The above job can rturn several partions, and in the following we execute a command line to merge all to one file
      ```shell
      $ hdfs dfs -cat /exp/pathToRstlts/part* | hdfs dfs -put - /exp/pathToADJ
     ```
# Start the clustering step
     ```shell
      $ spark-submit --deploy-mode cluster --num-executors 3 --executor-cores 2 --executor-memory 8GB --class DSCAN JarFile hdfs://master:9000/exp/pathToADJ hdfs://master:9000/exp/pathToFinalClustering
   ```
# Output data
The output result is also a text file, that contains in each line the VertexID and its clusterID, the next example for the output result 
VertexID:ClusterID Note that the -1 and -2 clusterID represent respectively the outliers and the bridges

 

