package de.mm.gradoop.operators;

import de.mm.gradoop.AbstractRunner;
import org.gradoop.flink.io.impl.dot.DOTDataSink;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

import java.time.LocalDateTime;

public class AddAgePropertyTransformation extends AbstractRunner {

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage: <inputPath> <outputPath>");
			return;
		}

		String inputPath = args[0];
		String outputPath = args[1];

		LogicalGraph inputGraph = readLogicalGraph(inputPath, "csv");

		LogicalGraph outputGraph = execute(inputGraph);

		writeLogicalGraph(outputGraph, outputPath, "csv");

//		DOTDataSink dotDataSink = new DOTDataSink(outputPath+"_simple", true, DOTDataSink.DotFormat.SIMPLE);
//		dotDataSink.write(outputGraph, true);
//		getExecutionEnvironment().execute();

		// Convert dot to png via graphviz
		// $ dot -Tpng filename.dot -o filename.png
	}

	// add 'age' field to persons based on current date
	private static LogicalGraph execute(LogicalGraph socialNetwork) {
		return socialNetwork
				.vertexInducedSubgraph(epgmVertex -> epgmVertex.getLabel().equalsIgnoreCase("person"))
				.transformVertices(
						(current, transformed) -> {
//							if (current.getLabel().equalsIgnoreCase("person")) {
								transformed.setLabel(current.getLabel());
								transformed.setProperties(current.getProperties());
								transformed.setProperty(
										"age",
										LocalDateTime.now().getYear() - current.getPropertyValue("birthday").getDate()
												.getYear());
								return transformed;
//							}
//							return current;
						}
				);
	}

}
