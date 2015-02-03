package flink.graphs.library;

import flink.graphs.Edge;
import flink.graphs.Graph;
import flink.graphs.GraphAlgorithm;
import flink.graphs.Vertex;

import flink.graphs.example.utils.PointerJumpingAggregator;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.BooleanValue;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@SuppressWarnings("serial")
public class MinSpanningTree implements GraphAlgorithm<Long, String, Double> {

    private final Integer maxIterations;

    private PointerJumpingAggregator aggregator;

    private static final String AGGREGATOR_NAME = "pointerJumpingAggregator";

    public MinSpanningTree(Integer maxIterations) {
        this.maxIterations = maxIterations;
        aggregator = new PointerJumpingAggregator();
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
                initialSolutionSet.iterateDelta(initialWorkSet, maxIterations, 0)
                        .registerAggregator(AGGREGATOR_NAME, aggregator);

        DataSet<Tuple5<Long, Long, Double, Long, Long>> fakeEdges = getFakeEdgesFromInitialWorkSet(iteration.getWorkset());
        DataSet<Tuple5<Long, Long, Double, Long, Long>> realEdges = getRealEdgesFromInitialWorkSet(iteration.getWorkset());

        // get the current root IDs from "fake" edges within the initial work-set
        DataSet<Vertex<Long, Long>> verticesWithRootIdsFromFakeEdges =
                assignSuperVertexIdsAsVertexValuesFromEdges(fakeEdges);

        DataSet<Tuple5<Long, Long, Double, Long, Long>> realEdgesWithUpdatedRootIds = updateRootIdsForRealEdges(realEdges,
                verticesWithRootIdsFromFakeEdges);

        DataSet<Tuple5<Long, Long, Double, Long, Long>> minEdges = minEdgePicking(realEdgesWithUpdatedRootIds);

        DataSet<Tuple1<Long>> superVertices = superVertexFinding(minEdges);

        // get the root IDs from the updated edges
        DataSet<Vertex<Long, Long>> verticesWithRootIdsFromRealEdges =
                assignSuperVertexIdsAsVertexValuesFromEdges(realEdgesWithUpdatedRootIds);

        // the supervertices will now have their ID as a value, while regular vertices get -1
        DataSet<Vertex<Long, Long>> verticesWithRootIDsFromVertices =
                assignSuperVertexIdsAsVertexValuesFromVertices(verticesWithRootIdsFromRealEdges, superVertices);

        DataSet<Vertex<Long, Long>> verticesWithLatestSuperVertexValues = getTheLatestSuperVertexValues(
                verticesWithRootIDsFromVertices, verticesWithRootIdsFromFakeEdges);

        // create the new "fake" edges
        DataSet<Edge<Long, Double>> fakeEdgesFromVertices =
                createFakeEdgesFromVertices(verticesWithRootIdsFromRealEdges);

        DataSet<Edge<Long, Double>> fakeEdgesFromEdges = createFakeEdgesFromMinEdges(minEdges);

        DataSet<Edge<Long, Double>> allFakeEdges = fakeEdgesFromVertices.union(fakeEdgesFromEdges);

        DataSet<Vertex<Long, Long>> verticesWithSuperVertices =
                computeRootIDsForAllVertices(verticesWithLatestSuperVertexValues,
                        allFakeEdges);

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
                }), realEdgesWithUpdatedRootIds.union(updateRootIDsForFakeEdges(allFakeEdges,
                        verticesWithSuperVertices)));

        DataSet<Tuple2<Tuple2<Long, Long>, Double>> resultFiltered = result.filter(new FilterFunction<Tuple2<Tuple2<Long, Long>, Double>>() {

            @Override
            public boolean filter(Tuple2<Tuple2<Long, Long>, Double> result) throws Exception {
                return !result.f0.f0.equals(-1L);
            }
        });

        DataSet<Edge<Long, Double>> resultedEdges = resultFiltered
                .map(new MapFunction<Tuple2<Tuple2<Long, Long>, Double>, Edge<Long, Double>>() {

                    @Override
                    public Edge<Long, Double> map(Tuple2<Tuple2<Long, Long>, Double> splittedEdge) throws Exception {

                        if(splittedEdge.f0.f0 < splittedEdge.f0.f1) {
                            return new Edge<Long, Double>(splittedEdge.f0.f0, splittedEdge.f0.f1, splittedEdge.f1);
                        } else {
                            return new Edge<Long, Double>(splittedEdge.f0.f1, splittedEdge.f0.f0, splittedEdge.f1);
                        }
                    }
                });

        return Graph.fromDataSet(undirectedGraph.getVertices(), resultedEdges.distinct(), result.getExecutionEnvironment());
    }

    /**
     * Function that creates the initial work-set.
     * @param undirectedGraph
     * @return a DataSet of Tuple5 containing:
     * 1). the edges along with their rootIDs for the source and target respectively
     * In the beginning, the rootIDs are equal to their corresponding vertexIDs
     * 2). fake edges in the form of (x, y, 0.0, rootIdX, rootIdY) where the node with id x
     * will determine the node with id y to have the same root value as x.
     * the weight "0.0" encodes the fact that we are dealing with a fake edge, hence the weight value
     * is negligible.
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

                }).union(undirectedGraph.getVertices().map(new MapFunction<Vertex<Long, String>,
                        Tuple5<Long, Long, Double, Long, Long>>() {

                    @Override
                    public Tuple5<Long, Long, Double, Long, Long> map(Vertex<Long, String> vertex) throws Exception {
                        return new Tuple5<Long, Long, Double, Long, Long>(vertex.f0, vertex.f0, new Double(0), vertex.f0,
                                vertex.f0);
                    }
                }));
    }

    /**
     * Retrieve the real edges from the delta iteration work-set which normally contains a union of real and
     * "fake" edges
     * @param workset
     * @return Tuple5 representing the real edges along with their srcVertexID and targetVertexID. The edges will
     * be part of separate groups identified by the rootIds
     */
    private DataSet<Tuple5<Long, Long, Double, Long, Long>> getRealEdgesFromInitialWorkSet(
            DataSet<Tuple5<Long, Long, Double, Long, Long>> workset) {

        return workset.filter(new FilterFunction<Tuple5<Long, Long, Double, Long, Long>>() {

            @Override
            public boolean filter(Tuple5<Long, Long, Double, Long, Long> tuple5) throws Exception {
                return !tuple5.f2.equals(new Double(0)) && !tuple5.f3.equals(tuple5.f4);
            }
        });
    }

    /**
     * Retrieve the "fake" edges from the delta iteration work-set which normally contains a union of real and
     * "fake" edges
     * @param workset
     * @return Tuple5 representing the "fake" edges along with their srcVertexID and targetVertexID
     */
    private DataSet<Tuple5<Long, Long, Double, Long, Long>> getFakeEdgesFromInitialWorkSet(
            DataSet<Tuple5<Long, Long, Double, Long, Long>> workset) {

        return workset.filter(new FilterFunction<Tuple5<Long, Long, Double, Long, Long>>() {

            @Override
            public boolean filter(Tuple5<Long, Long, Double, Long, Long> tuple5) throws Exception {
                return tuple5.f2.equals(new Double(0));
            }
        });
    }

    /**
     * The first phase of the DMST Algorithm. In parallel, each vertex picks its minimum weight edge.
     * @param workset
     * @return - a tuple5 representing the minimum weight edge and its src rootID, target rootID
     * If two edges have the same weight, ties are broken by picking the edge with the min rootID
     */
    public DataSet<Tuple5<Long, Long, Double, Long, Long>> minEdgePicking(
            DataSet<Tuple5<Long, Long, Double, Long, Long>> workset) {

        return workset.groupBy(3).minBy(2, 4);
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

                        if(edgeWithRootID1.f3 < edgeWithRootID2.f3) {
                            collector.collect(new Tuple1<Long>(edgeWithRootID1.f3));
                        }
                    }
                });
    }

    /**
     * At this stage of the implementation, we have the edges and their srcRootID, targetRootID
     * as well as the intermediate work-set.
     * This function returns a DataSet of vertices having their superVertexId as a value.
     * @param edges
     * @return
     */
    public DataSet<Vertex<Long, Long>> assignSuperVertexIdsAsVertexValuesFromEdges(
            DataSet<Tuple5<Long, Long, Double, Long, Long>> edges) {

        return edges.flatMap(new FlatMapFunction<Tuple5<Long, Long, Double, Long, Long>, Vertex<Long, Long>>() {

            @Override
            public void flatMap(Tuple5<Long, Long, Double, Long, Long> edgeWithRootID,
                                Collector<Vertex<Long, Long>> collector) throws Exception {

                collector.collect(new Vertex<Long, Long>(edgeWithRootID.f0, edgeWithRootID.f3));
                collector.collect(new Vertex<Long, Long>(edgeWithRootID.f1, edgeWithRootID.f4));
            }
        }).distinct();
    }

    /**
     * If, in a previous step, the coGroup converged, new vertex groups were formed and you need to
     * assign them an ID equal to the superVertex ID.
     * This "relabeling" is done in order to update the real edges once the coGrop no longer changes
     * the vertex roots for fake edges.
     * @param edges
     * @param verticesWithRootIDs
     * @return
     */
    public DataSet<Tuple5<Long, Long, Double, Long, Long>> updateRootIdsForRealEdges(
            DataSet<Tuple5<Long, Long, Double, Long, Long>> edges,
            DataSet<Vertex<Long, Long>> verticesWithRootIDs) {

        // join to find out the new srcRootIDs and targetRootIDs
        return edges.join(verticesWithRootIDs).where(0).equalTo(0)
                .with(new RichFlatJoinFunction<Tuple5<Long, Long, Double, Long, Long>,
                        Vertex<Long, Long>, Tuple5<Long, Long, Double, Long, Long>>() {

                    private Boolean needToUpdate = true;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        if(getIterationRuntimeContext().getSuperstepNumber() > 1) {
                            if(getIterationRuntimeContext().getPreviousIterationAggregate(AGGREGATOR_NAME)
                                    .equals(new BooleanValue(false))) {
                                needToUpdate = true;
                            } else {
                                needToUpdate = false;
                            }
                        } else {
                            needToUpdate = true;
                        }
                    }

                    @Override
                    public void join(Tuple5<Long, Long, Double, Long, Long> edgeWithRootIDs,
                                     Vertex<Long, Long> vertex,
                                     Collector<Tuple5<Long, Long, Double, Long, Long>> collector) throws Exception {

                        if(needToUpdate) {
                            collector.collect(new Tuple5<Long, Long, Double, Long, Long>(
                                    edgeWithRootIDs.f0, edgeWithRootIDs.f1, edgeWithRootIDs.f2, vertex.f1,
                                    edgeWithRootIDs.f4));
                        } else {
                            collector.collect(edgeWithRootIDs);
                        }
                    }
                })
                .join(verticesWithRootIDs).where(1).equalTo(0)
                .with(new RichFlatJoinFunction<Tuple5<Long, Long, Double, Long, Long>,
                        Vertex<Long, Long>, Tuple5<Long, Long, Double, Long, Long>>() {

                    private Boolean needToUpdate = true;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        if (getIterationRuntimeContext().getSuperstepNumber() > 1) {
                            if (getIterationRuntimeContext().getPreviousIterationAggregate(AGGREGATOR_NAME)
                                    .equals(new BooleanValue(false))) {
                                needToUpdate = true;
                            } else {
                                needToUpdate = false;
                            }
                        }
                        else{
                            needToUpdate = true;
                        }
                    }

                    @Override
                    public void join(Tuple5<Long, Long, Double, Long, Long> updatedEdgesSrc,
                                     Vertex<Long, Long> vertex,
                                     Collector<Tuple5<Long, Long, Double, Long, Long>> collector) throws Exception {

                        if (needToUpdate) {
                            collector.collect(new Tuple5<Long, Long, Double, Long, Long>(updatedEdgesSrc.f0,
                                    updatedEdgesSrc.f1, updatedEdgesSrc.f2, updatedEdgesSrc.f3, vertex.f1));
                        } else {
                            collector.collect(updatedEdgesSrc);
                        }
                    }
                })
                // remove edges that have the source and target in the same group(mimics the Edge Cleaning phase
                // of the DMST algorithm)
                .filter(new FilterFunction<Tuple5<Long, Long, Double, Long, Long>>() {

                    @Override
                    public boolean filter(Tuple5<Long, Long, Double, Long, Long> edgeWithRootIDs) throws Exception {

                        return !edgeWithRootIDs.f3.equals(edgeWithRootIDs.f4);
                    }
                });
    }

    /**
     * Function that assigns their own ID as a value to the supervertices
     * and -1 to the other vertices
     * @param verticesWithRootIDs
     * @return
     */
    public DataSet<Vertex<Long, Long>> assignSuperVertexIdsAsVertexValuesFromVertices(
            DataSet<Vertex<Long, Long>> verticesWithRootIDs, DataSet<Tuple1<Long>> superVertices) {

        return verticesWithRootIDs.coGroup(superVertices)
                .where(0).equalTo(0)
                .with(new CoGroupFunction<Vertex<Long, Long>, Tuple1<Long>, Vertex<Long, Long>>() {

                    @Override
                    public void coGroup(Iterable<Vertex<Long, Long>> iterableVertices,
                                        Iterable<Tuple1<Long>> iterableSuperVertices,
                                        Collector<Vertex<Long, Long>> collector) throws Exception {

                        Iterator<Vertex<Long, Long>> iteratorVertices = iterableVertices.iterator();
                        Iterator<Tuple1<Long>> iteratorSuperVertices = iterableSuperVertices.iterator();

                        if (iteratorSuperVertices.hasNext()) {
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
     * In case the coGroup did not modify all the superVertex values(i.e. some of them still have -1),
     * we need to consult the values provided by the fake edges since they contain the latest updates
     * @param verticesWithRootIDsFromVertices
     * @param verticesWithRootIdsFromFakeEdges
     * @return
     */
    private static DataSet<Vertex<Long, Long>> getTheLatestSuperVertexValues(
            DataSet<Vertex<Long, Long>> verticesWithRootIDsFromVertices,
            DataSet<Vertex<Long, Long>> verticesWithRootIdsFromFakeEdges) {

        return verticesWithRootIDsFromVertices.join(verticesWithRootIdsFromFakeEdges)
                .where(0).equalTo(0)
                .with(new RichFlatJoinFunction<Vertex<Long,Long>, Vertex<Long,Long>, Vertex<Long, Long>>() {

                    private Boolean needToUpdate = false;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        if (getIterationRuntimeContext().getSuperstepNumber() > 1) {
                            if (getIterationRuntimeContext().getPreviousIterationAggregate(AGGREGATOR_NAME)
                                    .equals(new BooleanValue(false))) {
                                needToUpdate = false;
                            } else {
                                needToUpdate = true;
                            }
                        }
                    }

                    @Override
                    public void join(Vertex<Long, Long> vertex,
                                     Vertex<Long, Long> vertexFromFakeEdge,
                                     Collector<Vertex<Long, Long>> collector) throws Exception {


                        if(needToUpdate) {
                            collector.collect(vertexFromFakeEdge);
                        } else {
                            collector.collect(vertex);
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
     * @param fakeEdges
     * @return
     */
    public DataSet<Vertex<Long, Long>> computeRootIDsForAllVertices(
            DataSet<Vertex<Long, Long>> verticesWithKnownSuperVertices,
            DataSet<Edge<Long, Double>> fakeEdges) {

        // it is our interest to make a coGroup with the vertices who do not yet
        // know the value of their supervertices
        DataSet<Vertex<Long, Long>> unsetVertices = filterUnsetVertices(verticesWithKnownSuperVertices);
        DataSet<Vertex<Long, Long>> setVertices = filterSetVertices(verticesWithKnownSuperVertices);

        DataSet<Edge<Long, Double>> fakeEdgesForSetVertices = fakeEdges.join(setVertices).where(0).equalTo(0)
                .with(new FlatJoinFunction<Edge<Long,Double>, Vertex<Long,Long>,Edge<Long, Double>>() {

                    @Override
                    public void join(Edge<Long, Double> edge, Vertex<Long, Long> vertex,
                                     Collector<Edge<Long, Double>> collector) throws Exception {

                        collector.collect(new Edge<Long, Double>(vertex.getValue(), edge.getTarget(), edge.getValue()));
                    }
                });

        DataSet<Vertex<Long, Long>> updatedUnsetVertices = unsetVertices.coGroup(fakeEdgesForSetVertices)
                .where(0).equalTo(1).with(new SupervertexCoGroupFunction());

        // vertices that keep -1 as their value
        DataSet<Vertex<Long, Long>> unsetVerticesThatNeedToBeUpdated = unsetVertices.coGroup(updatedUnsetVertices)
                .where(0).equalTo(0).with(new UnsetSuperVertexAfterUpdateCoGroupFunction());

        DataSet<Vertex<Long, Long>> finalResult  = setVertices.union(updatedUnsetVertices).union(unsetVerticesThatNeedToBeUpdated);

        return finalResult;
    }

    /**
     * Helper method for the computeRootIDsForAllVertices. It filters the vertices that have -1 as a value
     * (i.e. are not set)
     * @param verticesWithKnownSuperVertices
     * @return
     */
    private DataSet<Vertex<Long, Long>> filterUnsetVertices(DataSet<Vertex<Long, Long>> verticesWithKnownSuperVertices) {

        return verticesWithKnownSuperVertices.filter(new FilterFunction<Vertex<Long, Long>>() {

            @Override
            public boolean filter(Vertex<Long, Long> vertex) throws Exception {
                return vertex.f1.equals(new Long(-1));
            }
        });
    }

    /**
     * Helper method for the computeRootIDsForAllVertices. It filters the vertices that have their supervertex as a value
     * (i.e. are set)
     * @param verticesWithKnownSuperVertices
     * @return
     */
    private DataSet<Vertex<Long, Long>> filterSetVertices(DataSet<Vertex<Long, Long>> verticesWithKnownSuperVertices) {

        return verticesWithKnownSuperVertices.filter(new FilterFunction<Vertex<Long, Long>>() {

            @Override
            public boolean filter(Vertex<Long, Long> vertex) throws Exception {
                return !vertex.f1.equals(new Long(-1));
            }
        });
    }

    /**
     * The aggregator is false at the beginning.
     * Switch it to true when you do a collect within the coGroup
     */
    private static class SupervertexCoGroupFunction extends RichCoGroupFunction<Vertex<Long, Long>,
            Edge<Long, Double>, Vertex<Long, Long>> {

        private PointerJumpingAggregator aggregator;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            aggregator = getIterationRuntimeContext().getIterationAggregator(AGGREGATOR_NAME);
            aggregator.reset();
        }

        @Override
        public void coGroup(Iterable<Vertex<Long, Long>> iterableVertex,
                            Iterable<Edge<Long, Double>> iterableEdge,
                            Collector<Vertex<Long, Long>> collector) throws Exception {

            Iterator<Vertex<Long, Long>> iteratorVertex = iterableVertex.iterator();
            Iterator<Edge<Long, Double>> iteratorEdge = iterableEdge.iterator();

            if(iteratorVertex.hasNext()) {
                if(iteratorEdge.hasNext()) {
                    Edge<Long, Double> iteratorEdgeNext = iteratorEdge.next();

                    collector.collect(new Vertex(iteratorEdgeNext.f1, iteratorEdgeNext.f0));
                    // something has changed; modify the aggregator accordingly
                    aggregator.aggregate(new BooleanValue(true));
                }
            }
        }
    }

    private static class UnsetSuperVertexAfterUpdateCoGroupFunction extends RichCoGroupFunction<Vertex<Long, Long>,
            Vertex<Long, Long>, Vertex<Long, Long>> {

        private PointerJumpingAggregator aggregator;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            aggregator = getIterationRuntimeContext().getIterationAggregator(AGGREGATOR_NAME);
        }

        @Override
        public void coGroup(Iterable<Vertex<Long, Long>> iterableVertexUnset,
                            Iterable<Vertex<Long, Long>> iterableUpdatedVertex,
                            Collector<Vertex<Long, Long>> collector) throws Exception {

            Iterator<Vertex<Long, Long>> iteratorVertexUnset = iterableVertexUnset.iterator();
            Iterator<Vertex<Long, Long>> iteratorUpdatedVertex = iterableUpdatedVertex.iterator();

            if(iteratorVertexUnset.hasNext()) {
                if(!iteratorUpdatedVertex.hasNext()) {
                    collector.collect(iteratorVertexUnset.next());
                    // something has changed; modify the aggregator accordingly
                    aggregator.aggregate(new BooleanValue(true));
                }
            }
        }
    }

    /**
     * Function that mimics the Relabeling phase for fake edges.
     *
     * @param edges
     * @param verticesWithRootIDs
     * @return
     */
    public DataSet<Tuple5<Long, Long, Double, Long, Long>> updateRootIDsForFakeEdges(
            DataSet<Edge<Long, Double>> edges,
            DataSet<Vertex<Long, Long>> verticesWithRootIDs) {

        // join to find out the new srcRootIDs and targetRootIDs
        return edges.join(verticesWithRootIDs).where(0).equalTo(0)
                .with(new FlatJoinFunction<Edge<Long, Double>,
                        Vertex<Long, Long>, Tuple4<Long, Long, Double, Long>>() {

                    @Override
                    public void join(Edge<Long, Double> edge,
                                     Vertex<Long, Long> vertex,
                                     Collector<Tuple4<Long, Long, Double, Long>> collector) throws Exception {

                        collector.collect(new Tuple4<Long, Long, Double, Long>(
                                edge.f0, edge.f1, edge.f2, vertex.f1));
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
                });
    }


}
