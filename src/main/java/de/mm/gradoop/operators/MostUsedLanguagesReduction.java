package de.mm.gradoop.operators;

import de.mm.gradoop.AbstractRunner;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MostUsedLanguagesReduction extends AbstractRunner {

	public static void main(String[] args) throws Exception {

		if (args.length != 1) {
			System.err.println("Usage: <inputPath>");
			return;
		}
		String inputPath = args[0];
		LogicalGraph inputGraph = readLogicalGraph(inputPath, "csv");
		execute(inputGraph);
	}

	// reduce all person vertices to a single value with all spoken languages
	private static void execute(LogicalGraph socialNetwork) throws Exception {
		socialNetwork
				.getVertices()
				.filter(vertex -> vertex.getLabel().equalsIgnoreCase("person"))
				.reduce((v1, v2) -> {
					Set<PropertyValue> languages = new HashSet<>();
					languages.addAll(v1.getPropertyValue("speaks").getList());
					languages.addAll(v2.getPropertyValue("speaks").getList());
					v1.setProperty("speaks", new ArrayList<>(languages));
					return v1;
				})
				.collect()
				.forEach(vertex -> {
					List<PropertyValue> speaks = vertex.getPropertyValue("speaks").getList();
					Collections.sort(speaks);
					System.out.println(speaks);
				});
	}

}
