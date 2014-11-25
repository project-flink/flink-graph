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
* numberOfVertices()
* numberOfEdges()
* getVertexIds()
* getEdgeIds()
* isWeaklyConnected()
* addVertex()
* removeVertex()
* addEdge()
* union()
* passMessages()
* fromCollection(vertices, edges)
* fromCollection(edges)


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
* fromCollection(edges)

##Wishlist

###Graph Class
* getNeighborhoodGraph(Vertex src, int distance)
* vertexCentricComputation()
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
