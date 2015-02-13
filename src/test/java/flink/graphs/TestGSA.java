package flink.graphs;

import flink.graphs.gsa.ApplyFunction;
import flink.graphs.gsa.GatherFunction;
import flink.graphs.gsa.SumFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.util.Collector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

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

				GatherFunction<Long, Long, Long, HashSet<Vertex<Long,Long>>> gather =
						new GatherFunction<Long, Long, Long, HashSet<Vertex<Long,Long>>>() {

					@Override
					public Tuple2<Long, HashSet<Vertex<Long, Long>>> gather(Tuple3<Vertex<Long, Long>,
							Edge<Long, Long>, Vertex<Long, Long>> triplet) {

						HashSet<Vertex<Long, Long>> result = new HashSet<Vertex<Long, Long>>();
						result.add(triplet.f2);

						return new Tuple2<Long, HashSet<Vertex<Long, Long>>>(triplet.f0.getId(), result);
					}
				};

				SumFunction<Long, Long, Long, HashSet<Vertex<Long, Long>>> sum =
						new SumFunction<Long, Long, Long, HashSet<Vertex<Long, Long>>>() {

					@Override
					public Tuple2<Long, HashSet<Vertex<Long, Long>>> sum(
							Tuple2<Long, HashSet<Vertex<Long, Long>>> arg0,
							Tuple2<Long, HashSet<Vertex<Long, Long>>> arg1) {

						HashSet<Vertex<Long, Long>> result = new HashSet<Vertex<Long, Long>>();

						result.addAll(arg0.f1);
						result.addAll(arg1.f1);

						return new Tuple2<Long, HashSet<Vertex<Long, Long>>>(arg0.f0, result);
					}
				};

				ApplyFunction<Long, Long, Long, HashSet<Vertex<Long, Long>>> apply =
						new ApplyFunction<Long, Long, Long, HashSet<Vertex<Long, Long>>>() {

					@Override
					public void apply(Tuple2<Long, HashSet<Vertex<Long, Long>>> set,
									 Vertex<Long, Long> src, Collector<Vertex<Long, Long>> collector) {

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
				};

				Graph<Long, Long, Long> minColoring = graph.runGSAIteration(gather, sum, apply, 16);
				minColoring.getVertices().writeAsCsv(resultPath);

				env.execute();

				return
					"1,1\n" +
					"2,1\n" +
					"3,1\n" +
					"4,1\n" +
					"5,1\n";
			}
			default: 
				throw new IllegalArgumentException("Invalid program id");
			}
		}
	}
}
