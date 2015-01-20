package flink.graphs.library;

import flink.graphs.Edge;
import flink.graphs.Graph;
import flink.graphs.GraphAlgorithm;
import flink.graphs.Vertex;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@SuppressWarnings("serial")
public class MinSpanningTree implements GraphAlgorithm<Long, String, Double> {

    private final Integer maxIterations;

    public MinSpanningTree(Integer maxIterations) {
        this.maxIterations = maxIterations;
    }


    @Override
    public Graph<Long, String, Double> run(Graph<Long, String, Double> input) {
        Graph<Long, String, Double> undirectedGraph = input.getUndirected();

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // the third parameter of the delta iteration defines the key; if we choose to have
        // only the src as an identifier, the edge 1,2 and 1,6 will be considered the same and
        // will be fallaciously updated; therefore, we will keep both the src and the target as keys.
        final List<Tuple2<Tuple2<Long, Long>, Double>> splittedEdges = new ArrayList<>();
        splittedEdges.add(new Tuple2<Tuple2<Long, Long>, Double>(new Tuple2<Long, Long>(new Long(-1), new Long(-1)),
                new Double(-1)));

        // define the initial solution set (all the minEdges used throughout the algorithm)
        DataSet<Tuple2<Tuple2<Long, Long>, Double>> initialSolutionSet = env.fromCollection(splittedEdges);

        DataSet<Tuple5<Long, Long, Double, Long, Long>> initialWorkSet = assignInitialRootIds(undirectedGraph);

        // open a delta iteration
        DeltaIteration<Tuple2<Tuple2<Long, Long>, Double>, Tuple5<Long, Long, Double, Long, Long>> iteration =
                initialSolutionSet.iterateDelta(initialWorkSet, maxIterations, 0);

        DataSet<Tuple5<Long, Long, Double, Long, Long>> minEdges = minEdgePicking(iteration);

        DataSet<Tuple1<Long>> superVertices = superVertexFinding(minEdges);

        // initial vertices
        DataSet<Vertex<Long, Long>> intialVerticesWithRootIDsFromEdges =
                assignSuperVertexIdsAsVertexValuesFromEdges(iteration.getWorkset());

        DataSet<Vertex<Long, Long>> initialVerticesWithRootIDsFromVertices =
                assignSuperVertexIdsAsVertexValuesFromVertices(intialVerticesWithRootIDsFromEdges, superVertices);

        // create the initial "fake" edges
        DataSet<Edge<Long, Double>> fakeEdgesFromVertices =
                createFakeEdgesFromVertices(initialVerticesWithRootIDsFromVertices);

        DataSet<Edge<Long, Double>> fakeEdgesFromEdges = createFakeEdgesFromMinEdges(minEdges);

        DataSet<Vertex<Long, Long>> verticesWithSuperVertices =
                computeRootIDsForAllVertices(initialVerticesWithRootIDsFromVertices,
                        fakeEdgesFromVertices, fakeEdgesFromEdges, intialVerticesWithRootIDsFromEdges);

        // close the delta iteration
        DataSet<Tuple2<Tuple2<Long, Long>, Double>> result = iteration
                .closeWith(minEdges.map(new MapFunction<Tuple5<Long, Long, Double, Long, Long>,
                        Tuple2<Tuple2<Long, Long>, Double>>() {

                    @Override
                    public Tuple2<Tuple2<Long, Long>, Double> map(
                            Tuple5<Long, Long, Double, Long, Long> edgesWithRootIDs) throws Exception {

                        return new Tuple2<Tuple2<Long, Long>, Double>(new Tuple2<Long, Long>(
                                edgesWithRootIDs.f0, edgesWithRootIDs.f1),
                                edgesWithRootIDs.f2);
                    }
                }), edgeCleaning(iteration.getWorkset(), verticesWithSuperVertices));

        result = result.filter(new FilterFunction<Tuple2<Tuple2<Long, Long>, Double>>() {

            @Override
            public boolean filter(Tuple2<Tuple2<Long, Long>, Double> result) throws Exception {
                return !result.f0.f0.equals(-1L);
            }
        });

        DataSet<Edge<Long, Double>> resultedEdges = result
                .map(new MapFunction<Tuple2<Tuple2<Long, Long>, Double>, Edge<Long, Double>>() {
                    @Override
                    public Edge<Long, Double> map(Tuple2<Tuple2<Long, Long>, Double> splittedEdge) throws Exception {
                        return new Edge<Long, Double>(splittedEdge.f0.f0, splittedEdge.f0.f1, splittedEdge.f1);
                    }
                });

        resultedEdges = resultedEdges.coGroup(resultedEdges)
                .where(0,1).equalTo(1,0).with(new CoGroupFunction<Edge<Long, Double>, Edge<Long, Double>, Edge<Long, Double>>() {

                    @Override
                    public void coGroup(Iterable<Edge<Long, Double>> iterable1,
                                        Iterable<Edge<Long, Double>> iterable2,
                                        Collector<Edge<Long, Double>> collector) throws Exception {

                        Iterator<Edge<Long, Double>> iterator1 = iterable1.iterator();
                        Iterator<Edge<Long, Double>> iterator2 = iterable2.iterator();

                        if(iterator1.hasNext()) {
                            Edge<Long, Double> iterator1Next = iterator1.next();

                            if(iterator2.hasNext()) {

                                if(iterator1Next.f0 < iterator1Next.f1) {
                                    collector.collect(iterator1Next);
                                }
                            } else {
                                collector.collect(iterator1Next);
                            }
                        }
                    }
                });

        return Graph.fromDataSet(undirectedGraph.getVertices(), resultedEdges, result.getExecutionEnvironment());
    }

    /**
     * Function that creates the initial work-set.
     * @param undirectedGraph
     * @return a DataSet of Tuple5 containing the edges along with their rootIDs for the source and target respectively
     * In the beginning, the rootIDs are equal to their corresponding vertexIDs
     */
    private DataSet<Tuple5<Long, Long, Double, Long, Long>> assignInitialRootIds(
            Graph<Long, String, Double> undirectedGraph) {

        return undirectedGraph.getEdges()
                .map(new MapFunction<Edge<Long, Double>, Tuple5<Long, Long, Double, Long, Long>>() {

                    @Override
                    public Tuple5<Long, Long, Double, Long, Long> map(Edge<Long, Double> edge)
                            throws Exception {
                        return new Tuple5<Long, Long, Double, Long, Long>(edge.f0,
                                edge.f1, edge.f2, edge.f0, edge.f1);
                    }
                });
    }

    /**
     * The first phase of the DMST Algorithm. In parallel, each vertex picks its minimum weight edge.
     * @param iteration
     * @return - a tuple5 representing the minimum weight edge and its src rootID, target rootID
     * If two edges have the same weight, ties are broken by picking the edge with the min rootID
     */
    public DataSet<Tuple5<Long, Long, Double, Long, Long>> minEdgePicking(
            DeltaIteration<Tuple2<Tuple2<Long, Long>, Double>, Tuple5<Long, Long, Double, Long, Long>> iteration) {

        return iteration.getWorkset().groupBy(3).minBy(2, 4);
    }

    /**
     * Function that mimics the first answer phase of the SuperVertex Finding step:
     * Two nodes will discover that they are part of the same cojoined tree. Out of this pair,
     * the vertex with the smallest id will be designated as the root and will be returned.
     * @param minEdges - the minimum weight edges for all the vertices
     * @return - the id of the superVertex
     */
    public DataSet<Tuple1<Long>> superVertexFinding(DataSet<Tuple5<Long, Long, Double, Long, Long>> minEdges) {

        return minEdges.join(minEdges).where(3, 4).equalTo(4, 3)
                .with(new FlatJoinFunction<Tuple5<Long, Long, Double, Long, Long>,
                        Tuple5<Long, Long, Double, Long, Long>, Tuple1<Long>>() {

                    @Override
                    public void join(Tuple5<Long, Long, Double, Long, Long> edgeWithRootID1,
                                     Tuple5<Long, Long, Double, Long, Long> edgeWithRootID2,
                                     Collector<Tuple1<Long>> collector) throws Exception {

                        if (edgeWithRootID1.f3.compareTo(edgeWithRootID2.f3) < 0) {
                            collector.collect(new Tuple1<Long>(edgeWithRootID1.f3));
                        }
                    }
                });
    }

    /**
     * At this stage of the implementation, we have the minEdges and their srcRootID, targetRootID
     * as well as the intermediate work-set.
     * This function returns a DataSet of vertices having their superVertexId as a value.
     * @param edges
     * @return
     */
    public DataSet<Vertex<Long, Long>> assignSuperVertexIdsAsVertexValuesFromEdges(
            DataSet<Tuple5<Long, Long, Double, Long, Long>> edges) {

        return edges.map(new MapFunction<Tuple5<Long, Long, Double, Long, Long>, Vertex<Long, Long>>() {

            @Override
            public Vertex<Long, Long> map(Tuple5<Long, Long, Double, Long, Long> edgeWithRootID) throws Exception {
                return new Vertex<Long, Long>(edgeWithRootID.f0, edgeWithRootID.f3);
            }
        }).distinct();
    }

    /**
     * Function that assigns their own ID as a value to the supervertices
     * and -1 to the other vertices
     * @param initialVerticesWithRootIDs
     * @return
     */
    public DataSet<Vertex<Long, Long>> assignSuperVertexIdsAsVertexValuesFromVertices(
            DataSet<Vertex<Long, Long>> initialVerticesWithRootIDs, DataSet<Tuple1<Long>> superVertices) {

        return initialVerticesWithRootIDs.coGroup(superVertices)
                .where(0).equalTo(0)
                .with(new CoGroupFunction<Vertex<Long, Long>, Tuple1<Long>, Vertex<Long, Long>>() {

                    @Override
                    public void coGroup(Iterable<Vertex<Long, Long>> iterableVertices,
                                        Iterable<Tuple1<Long>> iterableSuperVertices,
                                        Collector<Vertex<Long, Long>> collector) throws Exception {

                        Iterator<Vertex<Long, Long>> iteratorVertices = iterableVertices.iterator();
                        Iterator<Tuple1<Long>> iteratorSuperVertices = iterableSuperVertices.iterator();

                        if(iteratorSuperVertices.hasNext()) {
                            Long iteratorSuperVerticesNext = iteratorSuperVertices.next().f0;

                            collector.collect(new Vertex<Long, Long>(iteratorSuperVerticesNext,
                                    iteratorSuperVerticesNext));
                        } else {
                            collector.collect(new Vertex<Long, Long>(iteratorVertices.next().f0, new Long(-1)));
                        }
                    }
                });
    }

    /**
     * Helper method that creates edges having the rootID as a src and
     * the id of the vertex which previously had this rootID as a src, as a target
     * @param verticesWithRootIDs
     * @return
     */
    public DataSet<Edge<Long, Double>> createFakeEdgesFromVertices(DataSet<Vertex<Long, Long>> verticesWithRootIDs) {

        return verticesWithRootIDs.flatMap(new FlatMapFunction<Vertex<Long, Long>, Edge<Long, Double>>() {

            @Override
            public void flatMap(Vertex<Long, Long> vertexWithRootId,
                                Collector<Edge<Long, Double>> collector) throws Exception {

                if(!vertexWithRootId.f0.equals(vertexWithRootId.f1)) {
                    collector.collect(new Edge<Long, Double>(vertexWithRootId.f1, vertexWithRootId.f0, new Double(0)));
                }
            }
        });
    }

    /**
     * Similar to createFakeEdgesFromVertices, this helper method creates a "fake" edge, this time, from the
     * minEdgeSrcRootIDTargetRootID tuple5.
     * @param minEdges
     * @return
     */
    public DataSet<Edge<Long, Double>> createFakeEdgesFromMinEdges(
            DataSet<Tuple5<Long, Long, Double, Long, Long>> minEdges) {

        return minEdges.map(new MapFunction<Tuple5<Long, Long, Double, Long, Long>, Edge<Long, Double>>() {

            @Override
            public Edge<Long, Double> map(Tuple5<Long, Long, Double, Long, Long> minEdgeSrcRootIDTargetRootID)
                    throws Exception {

                return new Edge<Long, Double>(minEdgeSrcRootIDTargetRootID.f4, minEdgeSrcRootIDTargetRootID.f3,
                        new Double(0));
            }
        });
    }

    /**
     * Having the supervertices and the fake edges, this function will fill in all the other vertex values
     * with respect to their supervertex values
     * @param verticesWithKnownSuperVertices
     * @param fakeEdgesFromVertices
     * @param fakeEdgesFromMinEdges
     * @param initialVerticesWithSuperVertices
     * @return
     */
    public DataSet<Vertex<Long, Long>> computeRootIDsForAllVertices(
            DataSet<Vertex<Long, Long>> verticesWithKnownSuperVertices,
            DataSet<Edge<Long, Double>> fakeEdgesFromVertices,
            DataSet<Edge<Long, Double>> fakeEdgesFromMinEdges,
            DataSet<Vertex<Long, Long>> initialVerticesWithSuperVertices) {

        DataSet<Edge<Long, Double>> fakeEdges = fakeEdgesFromVertices.union(fakeEdgesFromMinEdges);

        DataSet<Vertex<Long, Long>> finalResult = verticesWithKnownSuperVertices;

        for(int i = 0; i < 10; i++) {

            finalResult = finalResult.coGroup(fakeEdges)
                    .where(0).equalTo(0)
                    .with(new CoGroupFunction<Vertex<Long, Long>, Edge<Long, Double>, Vertex<Long, Long>>() {

                        @Override
                        public void coGroup(Iterable<Vertex<Long, Long>> iterableVertices,
                                            Iterable<Edge<Long, Double>> iterableFakeEdges,
                                            Collector<Vertex<Long, Long>> collector) throws Exception {

                            Iterator<Vertex<Long, Long>> iteratorVertices = iterableVertices.iterator();
                            Iterator<Edge<Long, Double>> iteratorFakeEdges = iterableFakeEdges.iterator();

                            if(iteratorVertices.hasNext()) {
                                Vertex<Long, Long> iteratorVerticesNext = iteratorVertices.next();

                                if(!iteratorVerticesNext.f1.equals(-1)) {
                                    collector.collect(iteratorVerticesNext);

                                    // collect the superVertex's neighbours as well
                                    while(iteratorFakeEdges.hasNext()) {

                                        collector.collect(new Vertex<Long, Long>(iteratorFakeEdges.next().f1,
                                                iteratorVerticesNext.f1));
                                    }
                                }
                            }

                        }
                    });
        }

        // coGroup with the initial Vertices in case the graph is not connected
        return finalResult.coGroup(initialVerticesWithSuperVertices).where(0).equalTo(0)
                .with(new CoGroupFunction<Vertex<Long, Long>, Vertex<Long, Long>, Vertex<Long, Long>>() {

                    @Override
                    public void coGroup(Iterable<Vertex<Long, Long>> iterableResult,
                                        Iterable<Vertex<Long, Long>> iterableInitialResult,
                                        Collector<Vertex<Long, Long>> collector) throws Exception {

                        Iterator<Vertex<Long, Long>> iteratorResult = iterableResult.iterator();
                        Iterator<Vertex<Long, Long>> iteratorInitialResult = iterableInitialResult.iterator();

                        if(iteratorResult.hasNext()) {
                            collector.collect(iteratorResult.next());
                        } else {
                            collector.collect(iteratorInitialResult.next());
                        }
                    }
                });
    }

    /**
     * Function that mimics the Edge Cleaning and Relabeling phase along with the final SuperVertex Formation
     * phase of the DMST algorithm.
     *
     * The vertices will be relabeled with the id of their supervertices while the edges with
     * srcRootID == targetRootID will be discarded.
     * @param edges
     * @param verticesWithRootIDs
     * @return
     */
    public DataSet<Tuple5<Long, Long, Double, Long, Long>> edgeCleaning(
            DataSet<Tuple5<Long, Long, Double, Long, Long>> edges,
            DataSet<Vertex<Long, Long>> verticesWithRootIDs) {

        // join to find out the new srcRootIDs and targetRootIDs
        return edges.join(verticesWithRootIDs).where(0).equalTo(0)
                .with(new FlatJoinFunction<Tuple5<Long, Long, Double, Long, Long>,
                        Vertex<Long, Long>, Tuple4<Long, Long, Double, Long>>() {

                    @Override
                    public void join(Tuple5<Long, Long, Double, Long, Long> edgeWithRootIDs,
                                     Vertex<Long, Long> vertex,
                                     Collector<Tuple4<Long, Long, Double, Long>> collector) throws Exception {

                        collector.collect(new Tuple4<Long, Long, Double, Long>(
                                edgeWithRootIDs.f0, edgeWithRootIDs.f1, edgeWithRootIDs.f2, vertex.f1));
                    }
                })
                .join(verticesWithRootIDs).where(1).equalTo(0)
                .with(new FlatJoinFunction<Tuple4<Long, Long, Double, Long>,
                        Vertex<Long, Long>, Tuple5<Long, Long, Double, Long, Long>>() {

                    @Override
                    public void join(Tuple4<Long, Long, Double, Long> updatedMinEdgesSrc,
                                     Vertex<Long, Long> vertex,
                                     Collector<Tuple5<Long, Long, Double, Long, Long>> collector) throws Exception {

                        collector.collect(new Tuple5<Long, Long, Double, Long, Long>(updatedMinEdgesSrc.f0,
                                updatedMinEdgesSrc.f1, updatedMinEdgesSrc.f2, updatedMinEdgesSrc.f3, vertex.f1));
                    }
                })
                .filter(new FilterFunction<Tuple5<Long, Long, Double, Long, Long>>() {

                    @Override
                    public boolean filter(Tuple5<Long, Long, Double, Long, Long> edgeWithRootIDs) throws Exception {

                        return !edgeWithRootIDs.f3.equals(edgeWithRootIDs.f4);
                    }
                });
    }

}
