package flink.graphs;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.util.Collector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import scala.collection.GenTraversableViewLike;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;

@RunWith(Parameterized.class)
public class TestGSA extends JavaProgramTestBase {

	private static int NUM_PROGRAMS = 1;

	private int curProgId = config.getInteger("ProgramId", -1);
	private String resultPath;
	private String expectedResult;

	public TestGSA(Configuration config) {
		super(config);
	}
	
	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void testProgram() throws Exception {
		expectedResult = GraphProgs.runProgram(curProgId, resultPath);
	}
	
	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(expectedResult, resultPath);
	}
	
	@Parameters
	public static Collection<Object[]> getConfigurations() throws FileNotFoundException, IOException {

		LinkedList<Configuration> tConfigs = new LinkedList<Configuration>();

		for(int i=1; i <= NUM_PROGRAMS; i++) {
			Configuration config = new Configuration();
			config.setInteger("ProgramId", i);
			tConfigs.add(config);
		}
		
		return toParameterList(tConfigs);
	}
	
	private static class GraphProgs {
		
		@SuppressWarnings("serial")
		public static String runProgram(int progId, String resultPath) throws Exception {
			
			switch(progId) {
			case 1: {
				/*
				 * Test mapVertices() keeping the same value type
				 */
				final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env),
						TestGraphUtils.getLongLongEdgeData(env), env);

				final int maxIterations = 16;
				DeltaIteration<Vertex<Long, Long>, Vertex<Long, Long>> iteration =
						graph.getVertices().iterateDelta(graph.getVertices(), maxIterations, 0);

				DataSet<Tuple3<Vertex<Long, Long>, Edge<Long, Long>, Vertex<Long, Long>>> triplets = iteration
						.getWorkset()
						.join(graph.getEdges()
								.join(graph.getVertices())
								.where(1)
								.equalTo(0))
						.where(0)
						.equalTo("f0.f0")
						.with(new FlatJoinFunction<Vertex<Long, Long>, Tuple2<Edge<Long, Long>, Vertex<Long, Long>>,
								Tuple3<Vertex<Long, Long>, Edge<Long, Long>, Vertex<Long, Long>>>() {

							@Override
							public void join(Vertex<Long, Long> vertex, Tuple2<Edge<Long, Long>, Vertex<Long, Long>>
									edgeVertex, Collector<Tuple3<Vertex<Long, Long>, Edge<Long, Long>,
									Vertex<Long, Long>>> collector) throws Exception {

								collector.collect(new Tuple3<Vertex<Long, Long>, Edge<Long, Long>, Vertex<Long, Long>>(
										vertex, edgeVertex.f0, edgeVertex.f1
								));
							}
						});

				DataSet<Tuple2<Long, HashSet<Vertex<Long, Long>>>> mappedTriplets = triplets
						.map(new MapFunction<Tuple3<Vertex<Long, Long>, Edge<Long, Long>, Vertex<Long, Long>>,
								Tuple2<Long, HashSet<Vertex<Long, Long>>>>() {

							@Override
							public Tuple2<Long, HashSet<Vertex<Long, Long>>> map(Tuple3<Vertex<Long, Long>,
									Edge<Long, Long>, Vertex<Long, Long>> triplet)
									throws Exception {

								HashSet<Vertex<Long, Long>> result = new HashSet<Vertex<Long, Long>>();
								result.add(triplet.f2);

								return new Tuple2<Long, HashSet<Vertex<Long, Long>>>(triplet.f0.getId(), result);
							}
						});

				DataSet<Tuple2<Long, HashSet<Vertex<Long, Long>>>> groupedTriplets = mappedTriplets
						.groupBy(0)
						.reduce(new ReduceFunction<Tuple2<Long, HashSet<Vertex<Long, Long>>>>() {
							@Override
							public Tuple2<Long, HashSet<Vertex<Long, Long>>> reduce(
									Tuple2<Long, HashSet<Vertex<Long, Long>>> arg0,
									Tuple2<Long, HashSet<Vertex<Long, Long>>> arg1) throws Exception {

								HashSet<Vertex<Long, Long>> result = new HashSet<Vertex<Long, Long>>();

								result.addAll(arg0.f1);
								result.addAll(arg1.f1);

								return new Tuple2<Long, HashSet<Vertex<Long, Long>>>(arg0.f0, result);
							}
						});

				DataSet<Vertex<Long, Long>> newVertices = groupedTriplets
						.join(graph.getVertices())
						.where(0)
						.equalTo(0)
						.with(new FlatJoinFunction<Tuple2<Long,HashSet<Vertex<Long,Long>>>,
								Vertex<Long,Long>, Vertex<Long, Long>>() {
							@Override
							public void join(Tuple2<Long, HashSet<Vertex<Long, Long>>> set,
											 Vertex<Long, Long> src, Collector<Vertex<Long, Long>> collector)
									throws Exception {

								// Find the minimum vertex id in the set which will be propagated
								long minValue = src.getValue();
								for (Vertex<Long, Long> v : set.f1) {
									if (v.getValue() < minValue) {
										minValue = v.getValue();
									}
								}

								if (minValue != src.getValue()) {
									collector.collect(new Vertex<Long, Long>(src.getId(), minValue));
								}
							}
						});

				DataSet<Vertex<Long, Long>> result = iteration.closeWith(newVertices, newVertices);
				result.print();

				/*
				Graph<Long, Long, Long> newGraph = graph.joinWithVertices(newVertices, new MapFunction<Tuple2<Long, Long>, Long>() {
					@Override
					public Long map(Tuple2<Long, Long> arg) throws Exception {
						return arg.f1;
					}
				});
				*/

				graph.getVertices().writeAsCsv(resultPath);
				env.execute();

				return
					"1,1\n" +
					"2,2\n" +
					"3,3\n" +
					"4,4\n" +
					"5,5\n";
			}
			default: 
				throw new IllegalArgumentException("Invalid program id");
			}
		}
	}
}
