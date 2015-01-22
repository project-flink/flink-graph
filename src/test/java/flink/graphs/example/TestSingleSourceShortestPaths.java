package flink.graphs.example;

import flink.graphs.example.utils.ExampleUtils;
import org.apache.flink.test.util.JavaProgramTestBase;

public class TestSingleSourceShortestPaths extends JavaProgramTestBase {

    private String resultPath;
    private String expectedResult;

    @Override
    protected void preSubmit() throws Exception {
        expectedResult = ExampleUtils.getSSSPVertexResult();
        resultPath = getTempDirPath("result");
    }

    @Override
    protected void testProgram() throws Exception {
        SingleSourceShortestPathsExample.main(resultPath, "5");
    }

    @Override
    protected void postSubmit() throws Exception {
        compareResultsByLinesInMemory(expectedResult, resultPath);
    }
}
