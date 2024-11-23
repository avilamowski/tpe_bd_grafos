package org.itba.grafos.tpe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
public class Utils {

    public static Dataset<Row> loadVerticesWithSchema(Iterable<Vertex> vIterator, SQLContext sqlContext) {
        List<StructField> vertexAtts = Arrays.stream(VertexPropsEnum.values())
                .map(VertexPropsEnum::toStructField)
                .collect(Collectors.toList());
        vertexAtts.add(0, DataTypes.createStructField("id", DataTypes.LongType, false));
        StructType vertexSchema = DataTypes.createStructType(vertexAtts);

        List<Row> vertexList = new ArrayList<>();
        List<String> ids = Arrays.stream(VertexPropsEnum.values())
                .map(VertexPropsEnum::getId)
                .collect(Collectors.toList());

        for (Vertex vertex : vIterator) {
            List<Object> properties = ids.stream()
                    .map(vertex::getProperty)
                    .collect(Collectors.toList());
            properties.add(0, Long.valueOf((String) vertex.getId()));
            vertexList.add(RowFactory.create(properties.toArray(new Object[0])));
        }

        return sqlContext.createDataFrame(vertexList, vertexSchema);
    }

    public static Dataset<Row> loadEdgesWithSchema(Iterable<Edge> eIterator, SQLContext sqlContext) {
        List<StructField> edgeAtts = Arrays.stream(EdgePropsEnum.values())
                .map(EdgePropsEnum::toStructField)
                .collect(Collectors.toList());
        edgeAtts.add(0, DataTypes.createStructField("id", DataTypes.LongType, false));
        edgeAtts.add(1, DataTypes.createStructField("src", DataTypes.LongType, false));
        edgeAtts.add(2, DataTypes.createStructField("dst", DataTypes.LongType, false));
        StructType edgeSchema = DataTypes.createStructType(edgeAtts);

        List<Row> edgeList = new ArrayList<>();
        List<String> ids = Arrays.stream(EdgePropsEnum.values())
                .map(EdgePropsEnum::getId)
                .collect(Collectors.toList());

        for (Edge edge : eIterator) {
            List<Object> properties = ids.stream()
                    .map(edge::getProperty)
                    .collect(Collectors.toList());
            properties.add(0, Long.valueOf((String) edge.getId()));
            properties.add(1, Long.valueOf((String) edge.getVertex(Direction.OUT).getId()));
            properties.add(2, Long.valueOf((String) edge.getVertex(Direction.IN).getId()));
            edgeList.add(RowFactory.create(properties.toArray(new Object[0])));
        }

        return sqlContext.createDataFrame(edgeList, edgeSchema);
    }

    public static void writeToFile(JavaRDD<String> formattedRDD, Path outputPath) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        fs.delete(outputPath, true);
        formattedRDD.saveAsTextFile(outputPath.toString());
    }
}
