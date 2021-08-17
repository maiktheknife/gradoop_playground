package de.mm.gradoop.ldbc;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.io.impl.graph.GraphDataSource;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.s1ck.ldbc.LDBCToFlink;
import org.s1ck.ldbc.tuples.LDBCEdge;
import org.s1ck.ldbc.tuples.LDBCVertex;

import java.util.*;
import java.util.stream.Collectors;

public class LDBCToGradoopConverter {

//		Das liest die ldbc generierten csv files ein und erzeugt LDBCVertices und LDBCEdges.
//		Wenn du die erzeugt hast, kannst du via GraphDataSource von Gradoop ImportVertices und ImportEdges erzeugen und den fertigen EPGM Graph erstellen lassen.
//		Den fertigen EPGMGraph schreibst du dann am besten via CSVSink erstmal weg.

private static Set<String> dataTypes = new HashSet<>();

public static void main(String[] args) throws Exception {
    String inputPath = args[0];
    String outputPath = args[1];

    ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

    System.err.println("HADOOP_CONF_DIR=" + System.getenv("HADOOP_CONF_DIR"));
    System.err.println("fs.defaultFS=" + new Configuration().get("fs.defaultFS"));
        System.err.println("fs.default.name=" + new Configuration().get("fs.default.name"));

        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(environment);
        LDBCToFlink ldbcToFlink = new LDBCToFlink(inputPath, environment);

        // read ldbc into flink dataset
        DataSet<LDBCVertex> vertices = ldbcToFlink.getVertices();
        DataSet<LDBCEdge> edges = ldbcToFlink.getEdges();

        //		System.out.println("#### Input ####");
        //		System.out.println("VertexCount: " + vertices.count());
        //		System.out.println("EdgeCount: " + edges.count());

        // transform formats
        DataSet<ImportVertex<Long>> importVertex =
                vertices.map((MapFunction<LDBCVertex, ImportVertex<Long>>) ldbcVertex -> {
                    Map<String, Object> cleanedProps = cleanMap(ldbcVertex.getProperties());
                    return new ImportVertex<Long>(ldbcVertex.f0, ldbcVertex.f1, Properties.createFromMap(cleanedProps));
                }).returns(new TypeHint<ImportVertex<Long>>() {
                    // NOOP
                });

        DataSet<ImportEdge<Long>> importEdges =
                edges.map((MapFunction<LDBCEdge, ImportEdge<Long>>) ldbcEdge -> {
                    Map<String, Object> cleanedProps = cleanMap(ldbcEdge.getProperties());
                    return new ImportEdge<Long>(ldbcEdge.getEdgeId(), ldbcEdge.getSourceVertexId(),
                            ldbcEdge.getTargetVertexId(),
                            ldbcEdge.getLabel(), Properties.createFromMap(cleanedProps));
                }).returns(new TypeHint<ImportEdge<Long>>() {

                });

        // create graph from input
        GraphDataSource<Long> dataSource = new GraphDataSource<>(importVertex, importEdges, config);
        LogicalGraph logicalGraph = dataSource.getLogicalGraph();

        //		System.out.println("#### Output ####");
        //		System.out.println("VertexCount: " + logicalGraph.getVertices().count());
        //		System.out.println("EdgeCount: " + logicalGraph.getEdges().count());

        // write graph as csv
        CSVDataSink csvDataSink = new CSVDataSink(outputPath, logicalGraph.getConfig());
        logicalGraph.writeTo(csvDataSink, true);

        try {
            environment.execute();
        } finally {
            System.out.println("Contained Datatypes");
            System.out.println(dataTypes);
        }
    }

    private static Map<String, Object> cleanMap(Map<String, Object> map) {
        HashMap<String, Object> hashMap = new HashMap<>(map);
        map.keySet().forEach(key -> {
            Object value = map.get(key);

            // Convert values contained in Lists to PropertyValues to avoid TransformationBug in Junghans ldbc-flink-import
            if (value instanceof List) {
                List<?> arrayList = (List) value;
                hashMap.put(key, arrayList.stream().map(PropertyValue::create).collect(Collectors.toList()));
            }

            dataTypes.add(value.getClass().getName());
        });
        return hashMap;
    }

}

