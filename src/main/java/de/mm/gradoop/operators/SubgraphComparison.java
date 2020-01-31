package de.mm.gradoop.operators;

import de.mm.gradoop.AbstractRunner;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.sampling.RandomVertexNeighborhoodSampling;
import org.gradoop.flink.model.impl.operators.sampling.functions.Neighborhood;

public class SubgraphComparison extends AbstractRunner {

	public static void main(String[] args) throws Exception {
		ParameterTool parameterTool = ParameterTool.fromArgs(args);
		ExecutionEnvironment.getExecutionEnvironment().getConfig().setGlobalJobParameters(parameterTool);

		String inputPath = parameterTool.get("in");
		String outputPath = parameterTool.get("out");

		LogicalGraph inputGraph = readLogicalGraph(inputPath, "csv");
		LogicalGraph outputGraph = execute(inputGraph);

		writeLogicalGraph(outputGraph, outputPath, "csv");
	}

	private static LogicalGraph execute(LogicalGraph socialNetwork) throws Exception {
		// 2199023266220|Chris|Jasani|female|1981-05-22|2010-03-29T22:50:40.375+0000|41.89.103.231|Firefox
		LogicalGraph sub1 = socialNetwork
				.subgraph(
						vertex -> vertex.getLabel().equalsIgnoreCase("person"),
						edge -> edge.getLabel().equalsIgnoreCase("knows"))
				.sample(new RandomVertexNeighborhoodSampling(0.01f, Neighborhood.BOTH));
//				.callForGraph(new GellyLabelPropagation(4, "person"));

		LogicalGraph sub2 = socialNetwork
				.subgraph(
						vertex -> vertex.getLabel().equalsIgnoreCase("person"),
						edge -> edge.getLabel().equalsIgnoreCase("knows"))
				.sample(new RandomVertexNeighborhoodSampling(0.01f, Neighborhood.BOTH));

		sub1.equalsByElementIds(sub2)
				.collect()
				.forEach(aBoolean ->
						System.out.print("equalsByData: " + aBoolean));

		return sub1;
	}

}