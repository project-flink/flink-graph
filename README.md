flink-graph
===========

Graph API for Apache Flink

##Implemented Operations

###Graph Class
* getVertices()
* getEdges()
* pga()
* create(vertices, edges)
* readTuple2CsvFile
* readEdgesCsvFile
* readGraphFromCsvFile
* addVertex()
* removeVertex()
* addEdge()
* union()

###Vertex Class

###Edge Class
* reverse()

##Tested Operations
* mapVertices()
* mapEdges()
* subGraph()
* outDegrees()
* getUndirected()
* reverse()
* fromCollection(vertices, edges)
* numberOfVertices()
* numberOfEdges()
* getVertexIds()
* getEdgeIds()
* isWeaklyConnected()
* runVertexCentricIteration()

##Wishlist

###Graph Class
* fromCollection(edges)
* getNeighborhoodGraph(Vertex src, int distance)
* edgeCentricComputation()
* partitionCentricComputation()

###Vertex Class
* getDegree()
* inDegree()
* outDegree()
* getInNeighbors()
* getOutNeighbors()
* getAllNeighbors()


###Edge Class

##Other (low priority)
* partitionBy
* sample
* centrality
* pagerank
* distance
* clusteringCoefficient
* dfs
* bfs
* sssp
* isIsomorphic
* isSubgraphOf
