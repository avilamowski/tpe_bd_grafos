package org.itba.grafos.tpe;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.IOException;

public class GraphLoader {

    private final Graph tinkerGraph;

    public GraphLoader() {
        this.tinkerGraph = new TinkerGraph();
    }

    public void loadGraphML(Path graphMLPath) throws Exception {
        if (graphMLPath == null) {
            throw new IllegalArgumentException("The graphMLPath cannot be null.");
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        if (!fs.exists(graphMLPath)) {
            throw new IOException("The file at " + graphMLPath + " does not exist.");
        }

        if (!fs.isFile(graphMLPath)) {
            throw new IOException("The path " + graphMLPath + " is not a file.");
        }

        try (FSDataInputStream inputStream = fs.open(graphMLPath)) {
            GraphMLReader reader = new GraphMLReader(tinkerGraph);
            reader.inputGraph(inputStream);
        } catch (IOException e) {
            throw new IOException("Error reading the GraphML file: " + e.getMessage(), e);
        }
    }

    public Dataset<Row> loadVertices(SQLContext sqlContext) {
        return Utils.loadVerticesWithSchema(tinkerGraph.getVertices(), sqlContext);
    }

    public Dataset<Row> loadEdges(SQLContext sqlContext) {
        return Utils.loadEdgesWithSchema(tinkerGraph.getEdges(), sqlContext);
    }
}
