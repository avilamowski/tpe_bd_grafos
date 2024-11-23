package org.itba.grafos.tpe;

import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;

import static org.itba.grafos.tpe.Utils.writeToFile;

import java.util.List;

public class Main {
    public static void main(String[] args) throws Exception {

        if (args == null || args.length != 1) {
            System.err.println("Usage: java -jar <program>.jar <path_to_graphml>");
            System.exit(1);
        }

        String inputPath = args[0];
        if (inputPath == null || inputPath.trim().isEmpty()) {
            System.err.println("Error: The input path cannot be null or empty.");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("TP Final Vilamowski");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        SparkSession session = SparkSession.builder().appName("TP Final Vilamowski").getOrCreate();
        SQLContext sqlContext = new SQLContext(session);

        GraphLoader graphLoader = new GraphLoader();
        Path path = new Path(args[0]);
        graphLoader.loadGraphML(path);

        Dataset<Row> verticesDF = graphLoader.loadVertices(sqlContext);
        Dataset<Row> edgesDF = graphLoader.loadEdges(sqlContext);

        GraphFrame myGraph = GraphFrame.apply(verticesDF, edgesDF);
        myGraph.triplets().createOrReplaceTempView("t_table");

        String timestamp = String.valueOf(System.currentTimeMillis());
        processQueryB1(sqlContext, new Path(path.getParent() + "/" + timestamp));
        processQueryB2(sqlContext, new Path(path.getParent() + "/" + timestamp));

        sparkContext.close();
    }

    private static void processQueryB1(SQLContext sqlContext, Path path) throws Exception {
        Dataset<Row> queryResult = sqlContext.sql(
    "SELECT DISTINCT " +
            "src.code AS origin, " +
            "NULL AS stop, " +
            "dst.code AS destination " +
            "FROM t_table " +
            "WHERE src.labelV = 'airport' AND dst.labelV = 'airport' " +
            "AND src.lat < 0 AND src.lon < 0 " +
            "AND dst.code = 'SEA' " +
            "AND src.code != 'SEA' " +
            "UNION " +
            "SELECT DISTINCT " +
            "t1.src.code AS origin, " +
            "t2.src.code AS stop, " +
            "t2.dst.code AS destination " +
            "FROM t_table t1 " +
            "JOIN t_table t2 " +
            "ON t1.dst.code = t2.src.code " +
            "WHERE t1.src.labelV = 'airport' AND t2.src.labelV = 'airport' AND t2.dst.labelV = 'airport' " +
            "AND t1.src.lat < 0 AND t1.src.lon < 0 " +
            "AND t2.dst.code = 'SEA'" +
            "AND t1.src.code != 'SEA' "
        );

        JavaRDD<String> formattedRDD = queryResult.javaRDD().map(row -> {
            String origin = row.getString(0);
            String stop = row.isNullAt(1) ? null : row.getString(1);
            String destination = row.getString(2);
            return stop == null
                    ? String.format("%s -> %s", origin, destination)
                    : String.format("%s -> %s -> %s", origin, stop, destination);
        });

        writeToFile(formattedRDD, new Path(path + "-b1.txt"));
    }

    private static void processQueryB2(SQLContext sqlContext, Path path) throws Exception {
        Dataset<Row> queryResult = sqlContext.sql(
    "SELECT t1.src.desc AS continent, " +
            "t2.src.code || ' (' || t2.src.desc || ')' AS country, " +
            "sort_array(collect_list(t2.dst.elev)) AS elevations " +
            "FROM t_table t1 " +
            "JOIN t_table t2 " +
            "ON t1.dst.code = t2.dst.code " +
            "WHERE t1.src.labelV = 'continent' AND t2.src.labelV = 'country' AND t2.dst.labelV = 'airport' " +
            "GROUP BY t1.src.desc, t2.src.code, t2.src.desc " +
            "ORDER BY t1.src.desc, t2.src.code"
        );

        JavaRDD<String> formattedRDD = queryResult.javaRDD().map(row -> {
            String continent = row.getString(0);
            String country = row.getString(1);
            List<Integer> elevations = row.getList(2);
            return String.format("%s %s %s", continent, country, elevations.toString());
        });

        writeToFile(formattedRDD, new Path(path + "-b2.txt"));
    }
}
