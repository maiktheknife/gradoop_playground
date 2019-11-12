package de.mm.gradoop.operators;

import de.mm.gradoop.AbstractRunner;
import org.gradoop.flink.io.impl.dot.DOTDataSink;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.sampling.RandomVertexSampling;
import org.gradoop.flink.model.impl.operators.sampling.SamplingAlgorithm;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;

public class GroupingExp extends AbstractRunner {

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage: <inputPath> <outputPath>");
			return;
		}

		String inputPath = args[0];
		String outputPath = args[1];

		LogicalGraph inputGraph = readLogicalGraph(inputPath, "csv");

		LogicalGraph outputGraph = execute(inputGraph);

//		writeLogicalGraph(outputGraph, outputPath, "csv");

		DOTDataSink dotDataSink = new DOTDataSink(outputPath+".dot", true, DOTDataSink.DotFormat.SIMPLE);
		dotDataSink.write(outputGraph, true);
		getExecutionEnvironment().execute();
	}

	// group people by age
	private static LogicalGraph execute(LogicalGraph socialNetwork) {
		return socialNetwork
				.vertexInducedSubgraph(vertex -> vertex.getLabel().equalsIgnoreCase("person"))
				.groupBy(Collections.singletonList("age"))
				.verify();
	}
}
