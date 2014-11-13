/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package flink.graphs;


import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation.ConstantFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ConstantFieldsFirst;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.*;


@SuppressWarnings("serial")
public class Graph<K extends Comparable<K> & Serializable, VV extends Serializable,
	EV extends Serializable> implements Serializable{

	private final DataSet<Tuple2<K, VV>> vertices;
	private final DataSet<Tuple3<K, K, EV>> edges;

	/** a graph is directed by default */
	private boolean isUndirected = false;


	public Graph(DataSet<Tuple2<K, VV>> vertices, DataSet<Tuple3<K, K, EV>> edges) {
		this.vertices = vertices;
		this.edges = edges;
	}

	public Graph(DataSet<Tuple2<K, VV>> vertices, DataSet<Tuple3<K, K, EV>> edges, boolean undirected) {
		this.vertices = vertices;
		this.edges = edges;
		this.isUndirected = undirected;
	}

	public DataSet<Tuple2<K, VV>> getVertices() {
		return vertices;
	}

	public DataSet<Tuple3<K, K, EV>> getEdges() {
		return edges;
	}

    /**
     * Apply a function to the attribute of each Tuple2 in the graph
     * @param mapper A function that transforms the attribute of each Tuple2
     * @return A DataSet of Tuple2 which contains the new values of all vertices
     */
	//TODO: support changing the vertex value type
    public DataSet<Tuple2<K, VV>> mapVertices(final MapFunction<VV, VV> mapper) {
        return vertices.map(new ApplyMapperToVertex<K, VV>(mapper));
    }
    
    private static final class ApplyMapperToVertex<K, VV> implements MapFunction
    	<Tuple2<K, VV>, Tuple2<K, VV>> {
    	
    	private MapFunction<VV, VV> innerMapper;
    	
    	public ApplyMapperToVertex(MapFunction<VV, VV> theMapper) {
    		this.innerMapper = theMapper;
    	}
    	
		public Tuple2<K, VV> map(Tuple2<K, VV> value) throws Exception {
			return new Tuple2<K, VV>(value.f0, innerMapper.map(value.f1));
		}    	
    }

    /**
     * Apply a function to the attribute of each Tuple3 in the graph
     * @param mapper A function that transforms the attribute of each Tuple3
     * @return A DataSet of Tuple3 which contains the new values of all edges
     */
    public <EV2 extends Serializable> DataSet<Tuple3<K, K, EV2>> mapEdges(final MapFunction<EV, EV2> mapper) {
        return edges.map(new ApplyMapperToEdge<K, EV, EV2>(mapper));
    }

    private class ApplyMapperToEdge<K, EV, EV2> implements MapFunction
            <Tuple3<K, K, EV>, Tuple3<K, K, EV2>> {

        private MapFunction<EV, EV2> innerMapper;

        public ApplyMapperToEdge(MapFunction<EV, EV2> theMapper) {
            this.innerMapper = theMapper;
        }

        public Tuple3<K, K, EV2> map(Tuple3<K, K, EV> value) throws Exception {
            return new Tuple3(value.f0, value.f1 ,innerMapper.map(value.f2));
        }
    }

    /**
     * Apply value-based filtering functions to the graph 
     * and return a sub-graph that satisfies the predicates
     * for both vertex values and edge values.
     * @param vertexFilter
     * @param edgeFilter
     * @return
     */
    public Graph<K, VV, EV> subgraph(FilterFunction<VV> vertexFilter, FilterFunction<EV> edgeFilter) {

        DataSet<Tuple2<K, VV>> filteredVertices = this.vertices.filter(
        		new ApplyVertexFilter<K, VV>(vertexFilter));

        DataSet<Tuple3<K, K, EV>> remainingEdges = this.edges.join(filteredVertices)
        		.where(0).equalTo(0)
        		.with(new ProjectEdge<K, VV, EV>())
        		.join(filteredVertices).where(1).equalTo(0)
        		.with(new ProjectEdge<K, VV, EV>());

        DataSet<Tuple3<K, K, EV>> filteredEdges = remainingEdges.filter(
        		new ApplyEdgeFilter<K, EV>(edgeFilter));

        return new Graph<K, VV, EV>(filteredVertices, filteredEdges);
    }
    
    @ConstantFieldsFirst("0->0;1->1;2->2")
    private static final class ProjectEdge<K, VV, EV> implements FlatJoinFunction<Tuple3<K,K,EV>, Tuple2<K,VV>, 
		Tuple3<K,K,EV>> {
		public void join(Tuple3<K, K, EV> first,
				Tuple2<K, VV> second, Collector<Tuple3<K, K, EV>> out) {
			out.collect(first);
		}
    }
    
    private static final class ApplyVertexFilter<K, VV> implements FilterFunction<Tuple2<K, VV>> {

    	private FilterFunction<VV> innerFilter;
    	
    	public ApplyVertexFilter(FilterFunction<VV> theFilter) {
    		this.innerFilter = theFilter;
    	}

		public boolean filter(Tuple2<K, VV> value) throws Exception {
			return innerFilter.filter(value.f1);
		}
    	
    }

    private static final class ApplyEdgeFilter<K, EV> implements FilterFunction<Tuple3<K, K, EV>> {

    	private FilterFunction<EV> innerFilter;
    	
    	public ApplyEdgeFilter(FilterFunction<EV> theFilter) {
    		this.innerFilter = theFilter;
    	}    	
        public boolean filter(Tuple3<K, K, EV> value) throws Exception {
            return innerFilter.filter(value.f2);
        }
    }

    /**
     * Return the out-degree of all vertices in the graph
     * @return A DataSet of Tuple2<vertexId, outDegree>
     */
    public DataSet<Tuple2<K, Long>> outDegrees() {

    	return vertices.join(edges).where(0).equalTo(0).map(new VertexKeyWithOne<K, EV, VV>())
    			.groupBy(0).sum(1);
    	}

    private static final class VertexKeyWithOne<K, EV, VV> implements
    	MapFunction<Tuple2<Tuple2<K, VV>, Tuple3<K, K, EV>>, Tuple2<K, Long>> {

		public Tuple2<K, Long> map(
				Tuple2<Tuple2<K, VV>, Tuple3<K, K, EV>> value) {
			return new Tuple2<K, Long>(value.f0.f0, 1L);
		}
    }
		
    /**
     * Push-Gather-Apply model of graph computation
     * @param cog
     * @param gred
     * @param fjoin
     * @param maxIterations
     * @param <MsgT>
     * @return
     */
    public <MsgT> Graph<K, VV, EV> pga(CoGroupFunction<Tuple2<K, VV>, Tuple3<K, K, EV>, Tuple2<K, MsgT>> cog,
                                       GroupReduceFunction<Tuple2<K, MsgT>, Tuple2<K, MsgT>> gred,
                                       FlatJoinFunction<Tuple2<K, MsgT>, Tuple2<K, VV>, Tuple2<K, VV>> fjoin,
                                       int maxIterations){

        DeltaIteration<Tuple2<K, VV>, Tuple2<K, VV>> iteration = this.vertices
            .iterateDelta(this.vertices, maxIterations, 0);

        DataSet<Tuple2<K, MsgT>> p = iteration.getWorkset().coGroup(this.edges).where(0).equalTo(0).with(cog);

        DataSet<Tuple2<K, MsgT>> g = p.groupBy(0).reduceGroup(gred);

        DataSet<Tuple2<K, VV>> a = g.join(iteration.getSolutionSet()).where(0).equalTo(0).with(fjoin);

        DataSet<Tuple2<K, VV>> result = iteration.closeWith(a, a);

        return new Graph<>(result, this.edges);
    }

	/**
	 * Convert the directed graph into an undirected graph
	 * by adding all inverse-direction edges.
	 *
	 */
	public Graph<K, VV, EV> getUndirected() throws UnsupportedOperationException {
		if (this.isUndirected) {
			throw new UnsupportedOperationException("The graph is already undirected.");
		}
		else {
			DataSet<Tuple3<K, K, EV>> undirectedEdges =
					edges.union(edges.map(new ReverseEdgesMap<K, EV>()));
			return new Graph<K, VV, EV>(vertices, undirectedEdges, true);
			}
	}

	@ConstantFields("0->1;1->0;2->2")
	private static final class ReverseEdgesMap<K, EV> implements MapFunction<Tuple3<K, K, EV>,
		Tuple3<K, K, EV>> {

		public Tuple3<K, K, EV> map(Tuple3<K, K, EV> value) {
			return new Tuple3<K, K, EV>(value.f1, value.f0, value.f2);
		}
	}

	/**
	 * Reverse the direction of the edges in the graph
	 * @return a new graph with all edges reversed
	 * @throws UnsupportedOperationException
	 */
	public Graph<K, VV, EV> reverse() throws UnsupportedOperationException {
		if (this.isUndirected) {
			throw new UnsupportedOperationException("The graph is already undirected.");
		}
		else {
			DataSet<Tuple3<K, K, EV>> undirectedEdges = edges.map(new ReverseEdgesMap<K, EV>());
			return new Graph<K, VV, EV>(vertices, (DataSet<Tuple3<K, K, EV>>) undirectedEdges, true);
		}
	}

	public static <K extends Comparable<K> & Serializable, VV extends Serializable,
		EV extends Serializable> Graph<K, VV, EV>
		create(DataSet<Tuple2<K, VV>> vertices, DataSet<Tuple3<K, K, EV>> edges) {
		return new Graph<K, VV, EV>(vertices, edges);
	}

	/**
	 * Read and create the graph Tuple2 dataset from a csv file
	 * @param env
	 * @param filePath
	 * @param delimiter
	 * @param Tuple2IdClass
	 * @param Tuple2ValueClass
	 * @return
	 */
	public static <K extends Comparable<K> & Serializable, VV extends Serializable>
		DataSet<Tuple2<K, VV>> readTuple2CsvFile(ExecutionEnvironment env, String filePath,
			char delimiter, Class<K> Tuple2IdClass, Class<VV> Tuple2ValueClass) {

		CsvReader reader = new CsvReader(filePath, env);
		DataSet<Tuple2<K, VV>> vertices = reader.fieldDelimiter(delimiter).types(Tuple2IdClass, Tuple2ValueClass)
		.map(new MapFunction<Tuple2<K, VV>, Tuple2<K, VV>>() {

			public Tuple2<K, VV> map(Tuple2<K, VV> value) throws Exception {
				return (Tuple2<K, VV>)value;
			}
		});
		return vertices;
	}

	/**
	 * Read and create the graph edge dataset from a csv file
	 * @param env
	 * @param filePath
	 * @param delimiter
	 * @param Tuple2IdClass
	 * @param edgeValueClass
	 * @return
	 */
	public static <K extends Comparable<K> & Serializable, EV extends Serializable>
		DataSet<Tuple3<K, K, EV>> readEdgesCsvFile(ExecutionEnvironment env, String filePath,
			char delimiter, Class<K> Tuple2IdClass, Class<EV> edgeValueClass) {

		CsvReader reader = new CsvReader(filePath, env);
		DataSet<Tuple3<K, K, EV>> edges = reader.fieldDelimiter(delimiter)
			.types(Tuple2IdClass, Tuple2IdClass, edgeValueClass)
			.map(new MapFunction<Tuple3<K, K, EV>, Tuple3<K, K, EV>>() {

			public Tuple3<K, K, EV> map(Tuple3<K, K, EV> value) throws Exception {
				return (Tuple3<K, K, EV>)value;
			}
		});
		return edges;
	}

	/**
	 * Create the graph, by reading a csv file for vertices
	 * and a csv file for the edges
	 * @param env
	 * @param Tuple2Filepath
	 * @param Tuple2Delimiter
	 * @param edgeFilepath
	 * @param edgeDelimiter
	 * @param Tuple2IdClass
	 * @param Tuple2ValueClass
	 * @param edgeValueClass
	 * @return
	 */
	public static <K extends Comparable<K> & Serializable, VV extends Serializable,
		EV extends Serializable> Graph<K, VV, EV> readGraphFromCsvFile(ExecutionEnvironment env,
				String Tuple2Filepath, char Tuple2Delimiter, String edgeFilepath, char edgeDelimiter,
				Class<K> Tuple2IdClass, Class<VV> Tuple2ValueClass,	Class<EV> edgeValueClass) {

		CsvReader Tuple2Reader = new CsvReader(Tuple2Filepath, env);
		DataSet<Tuple2<K, VV>> vertices = Tuple2Reader.fieldDelimiter(Tuple2Delimiter)
				.types(Tuple2IdClass, Tuple2ValueClass).map(new MapFunction<Tuple2<K, VV>,
						Tuple2<K, VV>>() {

			public Tuple2<K, VV> map(Tuple2<K, VV> value) throws Exception {
				return (Tuple2<K, VV>)value;
			}
		});

		CsvReader edgeReader = new CsvReader(edgeFilepath, env);
		DataSet<Tuple3<K, K, EV>> edges = edgeReader.fieldDelimiter(edgeDelimiter)
			.types(Tuple2IdClass, Tuple2IdClass, edgeValueClass)
			.map(new MapFunction<Tuple3<K, K, EV>, Tuple3<K, K, EV>>() {

			public Tuple3<K, K, EV> map(Tuple3<K, K, EV> value) throws Exception {
				return (Tuple3<K, K, EV>)value;
			}
		});

		return Graph.create(vertices, edges);
	}

    /**
     * Creates a graph from the given vertex and edge collections
     * @param env
     * @param v the collection of vertices
     * @param e the collection of edges
     * @return a new graph formed from the set of edges and vertices
     */
    public Graph<K, VV, EV> fromCollection(ExecutionEnvironment env, Collection<Tuple2<K, VV>> v,
                                           Collection<Tuple3<K, K, EV>> e) throws Exception {
        DataSet<Tuple2<K, VV>> vertices = env.fromCollection(v);
        DataSet<Tuple3<K, K, EV>> edges = env.fromCollection(e);

        return Graph.create(vertices, edges);
    }

    /**
     * Performs a Breadth First Search on a graph
     * @param src
     * @param env
     * @return
     */
//    public List<Tuple2<K, VV>> bfs(Tuple2<K, VV> src, ExecutionEnvironment env) {
//        List<Tuple2<K, VV>> neighbouringVertices = new ArrayList<>();
//        neighbouringVertices.add(src);
//        DataSet<Tuple2<K, VV>> verticesPreviousLevel = env.fromCollection(neighbouringVertices);
//        DataSet<Tuple2<K,VV>> allVertices = env.fromCollection(neighbouringVertices);
//
//        Map<Tuple2<K, VV>, Boolean> visited = new TreeMap<>();
//        visited.put(src, true);
//
//        while(true) {
//            /* for all the previuos level vertices, find the edges that were not yet visited */
//            DataSet<Tuple2<K, VV>> newVerticeLevel = verticesPreviousLevel.flatMap(new MapFunction<Tuple2<K, VV>,
//                    DataSet<Tuple2<K, VV>>>() {
//                @Override
//                public DataSet<Tuple2<K, VV>> map(Tuple2<K, VV> kvvTuple2) throws Exception {
//                    Graph.this.getEdges().filter(new FilterFunction<Tuple3<K, K, EV>>() {
//                    @Override
//                    public boolean filter(Tuple3<K, K, EV> kvvTuple2) throws Exception {
//                        return false;
//                    }
//                }).map(Tuple);
//            });
//        }
//
//    }

    /**
     * Creates a subgraph starting from a source vertex and expanding it for a given distance
     * (in number of edges)
     * @param src
     * @param distance
     * @return a  neighbourhood subgraph
     */
    public Graph<K, VV, EV> getNeighborhoodGraph(final Tuple2<K, VV> src, Integer distance, ExecutionEnvironment env) {
        Integer step = 0;
        List<Tuple3<K, K, EV>> edges = new ArrayList<>();
        DataSet<Tuple3<K, K, EV>> edgesForAVertex = env.fromCollection(edges);
        List<Tuple2<K, VV>> neighbouringVertices = new ArrayList<>();
        neighbouringVertices.add(src);
        DataSet<Tuple2<K,VV>> verticesPreviousLevel = env.fromCollection(neighbouringVertices);
        DataSet<Tuple2<K,VV>> allVertices = env.fromCollection(neighbouringVertices);

        final Map<K, Boolean> visited = new TreeMap<>();
//      visited.put(src.f0, true);

        while(step < distance) {
            /* for all the previuos level vertices, find the edges that were not yet visited */
            DataSet<DataSet<Tuple2<K, VV>>> newVerticeLevel = verticesPreviousLevel.flatMap(new FlatMapFunction<Tuple2<K, VV>, DataSet<Tuple2<K, VV>>>() {
                @Override
                public void flatMap(Tuple2<K, VV> kvvTuple2, Collector<DataSet<Tuple2<K, VV>>> dataSetCollector) throws Exception {
                   dataSetCollector.collect(null);
                }
            });

            edgesForAVertex = Graph.this.getEdges().filter(new FilterFunction<Tuple3<K, K, EV>>() {
                @Override
                public boolean filter(Tuple3<K, K, EV> kkevTuple3) throws Exception {
                    Tuple2<K, VV> currentVertex = src;

                    return ((kkevTuple3.f0.equals(currentVertex.f0))  ||
                            (kkevTuple3.f1.equals(currentVertex.f0)));
                }
            });
            step++;
        }
    }

}
