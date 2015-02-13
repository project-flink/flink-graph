Gelly
===========

Graph API for Apache Flink

**Note:** This project [has been merged](https://github.com/apache/flink/pull/335) into the [apache flink repository](https://github.com/apache/flink/tree/master/flink-staging/flink-gelly).

- Development continues there.
- Issues are now here: https://issues.apache.org/jira/browse/FLINK-1536?jql=project%20%3D%20FLINK%20AND%20component%20%3D%20Gelly

##Implemented Methods

###Graph Creation

* create
* fromCollection

###Graph Properties and Metrics

* getVertices
* getEdges
* getVertexIds
* getEdgeIds
* numberOfVertices
* numberOfEdges
* getDegrees
* inDegrees
* outDegrees
* isWeaklyConnected

###Graph Mutations

* addVertex
* addEdge
* removeVertex
* removeEdge

###Graph Transformations

* mapVertices
* mapEdges
* union
* filterOnVertices
* filterOnEdges
* subgraph
* reverse
* getUndirected
* joinWithVertices
* joinWithEdges
* joinWithEdgesOnSource
* joinWithEdgesOnTarget


### Neighborhood Methods

* reduceOnEdges
* reduceOnNeighbors

### Graph Validation

* validate
* InvalidVertexIdsValidator

## Graph Algorithms

* PageRank
* SingleSourceShortestPaths

## Examples

* GraphMetrics
* PageRank
* SingleSourceShortestPaths
