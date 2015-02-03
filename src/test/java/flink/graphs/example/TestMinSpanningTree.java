package flink.graphs.example;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import flink.graphs.example.utils.MinSpanningTreeData;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;

@RunWith(Parameterized.class)
public class TestMinSpanningTree extends MultipleProgramsTestBase {

    private String verticesPath;
    private String edgesPath;
    private String resultPath;
    private String expected;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    public TestMinSpanningTree(ExecutionMode mode) {
        super(mode);
    }

    @Before
    public void before() throws Exception {
        resultPath = tempFolder.newFile().toURI().toString();
        File verticesFile = tempFolder.newFile();
        Files.write(MinSpanningTreeData.VERTICES, verticesFile, Charsets.UTF_8);

        File edgesFile = tempFolder.newFile();
        Files.write(MinSpanningTreeData.EDGES, edgesFile, Charsets.UTF_8);

        verticesPath = verticesFile.toURI().toString();
        edgesPath = edgesFile.toURI().toString();
    }

    @Test
    public void testMSTExample() throws Exception {
        MinSpanningTreeExample.main(new String[] {verticesPath, edgesPath, resultPath, MinSpanningTreeData.NUM_VERTICES+""});
        expected = MinSpanningTreeData.RESULTED_MIN_SPANNING_TREE;
    }

    @After
    public void after() throws Exception {
         compareResultsByLinesInMemory(expected, resultPath);
    }
}
