package de.mm.gradoop.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

public class WordCount {

	public static void main(String[] args) throws Exception {

		ParameterTool parameterTool = ParameterTool.fromArgs(args);
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.getConfig().setGlobalJobParameters(parameterTool);

		String inputPath = parameterTool.get("in");
		String outputPath = parameterTool.get("out");

		DataSource<String> inputText = env.readTextFile(inputPath);

		DataSet<Tuple2<String, Integer>> counts =
				// split up the lines in pairs (2-tuples) containing: (word,1)
				// group by the tuple field "0" and sum up tuple field "1"
				inputText
						.flatMap(new Tokenizer())
						.groupBy(0)
						.sum(1);

		counts.writeAsText(outputPath);

		// execute program
		env.execute("WordCount");
	}

	public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<>(token, 1));
				}
			}
		}
	}
}
