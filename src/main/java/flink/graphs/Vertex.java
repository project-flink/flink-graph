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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class Vertex<K extends Comparable<K> & Serializable, V extends Serializable> extends Tuple2<K, V> {

	private static final long serialVersionUID = 1L;

	public Vertex(){}

	public Vertex(K k, V val) {
		this.f0 = k;
		this.f1 = val;
	}

	public K getId() {
		return this.f0;
	}

	public V getValue() {
		return this.f1;
	}

	public void setId(K id) {
		this.f0 = id;
	}

	public void setValue(V val) {
		this.f1 = val;
	}

    /**
     * Counts the number of edges that respect the given filter function
     * @param graph the initial graph
     * @param f the function used for filtering
     * @return  a counter representing the edges that respect f
     */
    public <EV extends Serializable> DataSet<Integer> countWhere (Graph<K, V, EV> graph, FilterFunction<Tuple3<K, K, EV>> f) {
        ExecutionEnvironment environment = graph.getEdges().getExecutionEnvironment();
        List<Integer> list = new ArrayList<>();
        list.add(0);
        DataSet<Integer> initialCount = environment.fromCollection(list);

        return graph.getEdges().filter(f).map(new MapFunction<Tuple3<K, K, EV>, Integer>() {
            public Integer map(Tuple3<K, K, EV> edge){
                return 1;
            }
        }).union(initialCount).reduce(new ReduceFunction<Integer>() {
            public Integer reduce(Integer val1, Integer val2) {
                return val1 + val2;
            }
        });
    }

    /**
     * @param graph the initial graph
     * @return a counter representing the number of edges that enter a vertex
     */
    public <EV extends Serializable> DataSet<Integer> inDegree(Graph<K, V, EV> graph) {
        return countWhere(graph, new FilterFunction<Tuple3<K, K, EV>>() {
            @Override
            public boolean filter(Tuple3<K, K, EV> edge) throws Exception {
                return Vertex.this.getId().equals(edge.f1);
            }
        });
    }

    /**
     * @param graph the initial graph
     * @return a counter representing the number of edges that leave a vertex
     */
    public <EV extends Serializable> DataSet<Integer> outDegree(Graph<K, V, EV> graph) {
        return countWhere(graph, new FilterFunction<Tuple3<K, K, EV>>() {
            @Override
            public boolean filter(Tuple3<K, K, EV> edge) throws Exception {
                return Vertex.this.getId().equals(edge.f0);
            }
        });
    }

    /**
     * @param graph the initial graph
     * @return a counter representing the number of edges that either enter or leave a vertex
     */
    public <EV extends Serializable> DataSet<Integer> getDegree(Graph<K, V, EV> graph) {
        return countWhere(graph, new FilterFunction<Tuple3<K, K, EV>>() {
            @Override
            public boolean filter(Tuple3<K, K, EV> edge) throws Exception {
                return (Vertex.this.getId().equals(edge.f0)) || (Vertex.this.getId().equals(edge.f1));
            }
        });
    }

    /**
     * @param graph the initial graph
     * @return the set of ingoing neighbouring edges
     */
    public <EV extends Serializable> DataSet<K> getInNeighbors(Graph<K, V, EV> graph) {
        return graph.getEdges().filter(new FilterFunction<Tuple3<K, K, EV>>() {
            @Override
            public boolean filter(Tuple3<K, K, EV> edge) throws Exception {
                return Vertex.this.getId().equals(edge.f1);
            }
        }).map(new MapFunction<Tuple3<K, K, EV>, K>() {
            public K map(Tuple3<K, K, EV> edge){
                return edge.f0;
            }
        });
    }

    /**
     * @param graph the initial graph
     * @return the set of outgoing neighbouring edges
     */
    public <EV extends Serializable> DataSet<K> getOutNeighbors(Graph<K, V, EV> graph) {
        return graph.getEdges().filter(new FilterFunction<Tuple3<K, K, EV>>() {
            @Override
            public boolean filter(Tuple3<K, K, EV> edge) throws Exception {
                return Vertex.this.getId().equals(edge.f0);
            }
        }).map(new MapFunction<Tuple3<K, K, EV>, K>() {
            public K map(Tuple3<K, K, EV> edge){
                return edge.f1;
            }
        });
    }

    /**
     * @param graph the initial graph
     * @return the set of all neighbouring edges
     */
    public <EV extends Serializable> DataSet<K> getAllNeighbors(Graph<K, V, EV> graph) {
        return graph.getEdges().filter(new FilterFunction<Tuple3<K, K, EV>>() {
            @Override
            public boolean filter(Tuple3<K, K, EV> edge) throws Exception {
                return (Vertex.this.getId().equals(edge.f0)) || (Vertex.this.getId().equals(edge.f1));
            }
        }).map(new MapFunction<Tuple3<K, K, EV>, K>() {
            public K map(Tuple3<K, K, EV> edge){
                if(Vertex.this.getId().equals(edge.f0)) return edge.f1;
                else return edge.f0;
            }
        });
    }
}
