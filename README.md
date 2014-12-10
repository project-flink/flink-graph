flink-graph
===========

Graph API for Apache Flink

##Implemented Operations

###Graph Class
* getNeighbors(vertexId)
* getInNeighbors(vertexId)
* getOutNeighbors(vertexId)

###Vertex Class

###Edge Class
* reverse()

##Tested Operations
* mapVertices()
* mapEdges()
* subgraph()
* filterOnVertices(vertexFilter)
* filterOnEdges(edgeFilter)
* outDegrees()
* inDegrees()
* getDegrees()
* getUndirected()
* reverse()
* addVertex()
* removeVertex()
* addEdge()
* removeEdge()
* union()
* isWeaklyConnected()
* runVertexCentricIteration()
* fromCollection(vertices, edges)
* getVertices()
* getEdges()
* create(vertices, edges)
* numberOfVertices()
* numberOfEdges()
* getVertexIds()
* getEdgeIds()
* fromCollection(edges)
* getNeighborhoodGraph(Vertex src, int distance)
* vertexCentricComputation()

##Wishlist

###Graph Class
* edgeCentricComputation()
* partitionCentricComputation()

###Vertex Class
* getDegree()
* inDegree()
* outDegree()

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
