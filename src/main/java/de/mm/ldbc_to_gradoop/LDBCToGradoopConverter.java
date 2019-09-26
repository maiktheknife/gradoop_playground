package de.mm.ldbc_to_gradoop;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.io.impl.graph.GraphDataSource;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.s1ck.ldbc.LDBCToFlink;
import org.s1ck.ldbc.tuples.LDBCEdge;
import org.s1ck.ldbc.tuples.LDBCVertex;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

public class LDBCToGradoopConverter {

//		Das liest die ldbc generierten csv files ein und erzeugt LDBCVertices und LDBCEdges.
//		Wenn du die erzeugt hast, kannst du via GraphDataSource von Gradoop ImportVertices und ImportEdges erzeugen und den fertigen EPGM Graph erstellen lassen.
//		Den fertigen EPGMGraph schreibst du dann am besten via CSVSink erstmal weg.

	public static void main(String[] args) throws Exception {
		String inputPath = args[0];
		String outputPath = args[1];

		ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

		GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(environment);
		LDBCToFlink ldbcToFlink = new LDBCToFlink(inputPath, environment);

		// read ldbc into flink dataset
		DataSet<LDBCVertex> vertices = ldbcToFlink.getVertices();
		DataSet<LDBCEdge> edges = ldbcToFlink.getEdges();

		System.out.println("#### Input ####");
		System.out.println("VertexCount: " +vertices.count());
		System.out.println("EdgeCount: " + edges.count());

		// transform formats
		DataSet<ImportVertex<Long>> importVertex =
				vertices.map(new MapFunction<LDBCVertex, ImportVertex<Long>>() {
					@Override
					public ImportVertex<Long> map(LDBCVertex ldbcVertex) throws Exception {
						Map<String, Object> cleanedProps = cleanMap(ldbcVertex.getProperties());
						return new ImportVertex<Long>(ldbcVertex.f0, ldbcVertex.f1, Properties.createFromMap(cleanedProps));
					}
				});

		DataSet<ImportEdge<Long>> importEdges =
				edges.map(new MapFunction<LDBCEdge, ImportEdge<Long>>() {
					@Override
					public ImportEdge<Long> map(LDBCEdge ldbcEdge) throws Exception {
						Map<String, Object> cleanedProps = cleanMap(ldbcEdge.getProperties());
						return new ImportEdge<Long>(ldbcEdge.getEdgeId(), ldbcEdge.getSourceVertexId(), ldbcEdge.getTargetVertexId(),
								ldbcEdge.getLabel(), Properties.createFromMap(cleanedProps));
					}
				});

		// create graph from input
		GraphDataSource<Long> dataSource = new GraphDataSource<>(importVertex, importEdges, config);
		LogicalGraph logicalGraph = dataSource.getLogicalGraph();

		System.out.println("#### Output ####");
		System.out.println("VertexCount: " +logicalGraph.getVertices().count());
		System.out.println("EdgeCount: " +logicalGraph.getEdges().count());

		// write graph as csv
		CSVDataSink csvDataSink = new CSVDataSink(outputPath, logicalGraph.getConfig());
		logicalGraph.writeTo(csvDataSink, true);

		environment.execute();
	}

	private static LocalDateTime convertToLocalDateTimeViaInstant(Date dateToConvert) {
		return dateToConvert.toInstant()
				.atZone(ZoneId.systemDefault())
				.toLocalDateTime();
	}

	private static Map<String, Object> cleanMap(Map<String, Object> map){
		HashMap<String, Object> hashMap = new HashMap<>();
		map.keySet().forEach(key -> {
			Object value = map.get(key);
			if (Date.class.equals(value.getClass())) {
				value = convertToLocalDateTimeViaInstant((Date) value);
				hashMap.put(key, value);
			}

			// TODO: this should not be necessary
			if (ArrayList.class.equals(value.getClass())) {
				// remove list fields
				hashMap.remove(key);
			}
		});
		return hashMap;
	}

}

