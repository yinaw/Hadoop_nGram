import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NGramLibraryBuilder {
	public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		int noGram;
		@Override
		public void setup(Context context) {
			//how to get n-gram from command line?
			Configuration conf = context.getConfiguration();
			noGram = conf.getInt("noGram", 5);
		}

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = value.toString();
			
			line = line.trim().toLowerCase();

			//how to remove useless elements?
			line = line.replaceAll("[^a-z]", " ");

			//how to separate word by space?
			String[] words = line.split("\\s+");

			//how to build n-gram based on array of words?
			int length = words.length;

			for (int i = 0; i < length - 1; i++) {
				StringBuilder stringBuilder = new StringBuilder();
				stringBuilder.append(words[i]);

				for (int j = 1; i + j < length && j < noGram; j ++) {
					stringBuilder.append(" ");
					stringBuilder.append(words[i + j]);
					context.write(new Text(stringBuilder.toString().trim()), new IntWritable(1));
				}
			}
		}
	}

	public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0;

			for (IntWritable value: values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
			//how to sum up the total count for each n-gram?
		}
	}

}