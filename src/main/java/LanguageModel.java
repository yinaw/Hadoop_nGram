import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.*;
import org.apache.hadoop.util.PriorityQueue;

import java.io.IOException;
import java.util.*;

public class LanguageModel {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		int threashold;

		@Override
		public void setup(Context context) {
			threashold = context.getConfiguration().getInt("threashold", 20);
			// how to get the threashold parameter from the configuration?
		}

		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if((value == null) || (value.toString().trim()).length() == 0) {
				return;
			}
			//this is cool\t20   ---> value
			String line = value.toString().trim();
			
			String[] wordsPlusCount = line.split("\t");
			if(wordsPlusCount.length < 2) {
				return;
			}
			
			String[] words = wordsPlusCount[0].split("\\s+");   //all kinds of seperators.
			int count = Integer.valueOf(wordsPlusCount[1]);

			//how to filter the n-gram lower than threashold
			if (count < threashold) {
				return;
			}

			StringBuilder stringBuilder = new StringBuilder();

			for (int i = 0; i < words.length - 1; i++) {
				stringBuilder.append(" ");
			}

			String outputKey = stringBuilder.toString().trim();
			String outputValue = words[words.length - 1] + "=" + count;

			if (!(outputKey == null) || outputKey.length() < 1) {
				context.write(new Text(outputKey), new Text(outputValue));
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

		int topn;
		// get the n parameter from the configuration
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			topn = conf.getInt("n", 5);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			PriorityQueue<String> priorityQueue = new PriorityQueue<String>() {
				@Override
				protected boolean lessThan(Object o, Object o1) {
					String word = ((String)o).split("=")[0].trim();
					int count = Integer.parseInt(word.split("=")[1].trim());

					String word1 = ((String)o1).split("=")[0].trim();
					int count1 = Integer.parseInt(word1.split("=")[1].trim());

					return count - count1 > 0;
				}
			};
			for (Text val : values) {
				String curValue = val.toString().trim();

				if (priorityQueue.size() < topn) {
					priorityQueue.put(curValue);
				}
				else {
					priorityQueue.put(curValue);
					priorityQueue.pop();
				}
			}

			for (int j = 0; j < priorityQueue.size(); j++) {
				String value = priorityQueue.pop();

				String word = value.split("=")[0].trim();
				int count = Integer.parseInt(word.split("=")[1].trim());

				context.write(new DBOutputWritable(key.toString(), word, count),NullWritable.get());
			}
			
		}
	}
}
