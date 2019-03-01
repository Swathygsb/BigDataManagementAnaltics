import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MutualFriends {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		final Logger log = LoggerFactory.getLogger(Map.class);

		private Text coupleKey = new Text(); // output key is of text type, holds personA, Person B

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] docLine = value.toString().split("\t");
			if (docLine.length == 2) {
				String[] friendList = docLine[1].split(",");
				for (String data : friendList) {
					String friendPairKey = "";
					if (Integer.parseInt(docLine[0]) > Integer.parseInt(data))
						friendPairKey = data + ", " + docLine[0];
					else
						friendPairKey = docLine[0] + ", " + data;

					coupleKey.set(friendPairKey);
					context.write(coupleKey, new Text(docLine[1]));
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// values will be maximum size two , list of (a,b) and (a,b) from (b,a)
			HashSet<String> friendsList1 = new HashSet<>();
			HashSet<String> friendsList2 = new HashSet<>();

			int index = 1;
			for (Text val : values) {
				if (index == 1) {
					friendsList1.addAll(Arrays.asList(val.toString().split(",")));

				} else if (index == 2) {
					friendsList2.addAll(Arrays.asList(val.toString().split(",")));
				}
				index++;
			}

			// finding common items of two friends list
			friendsList1.retainAll(friendsList2);

			StringBuilder sb = new StringBuilder();
			for (String s : friendsList1) {
				sb.append(s).append(",");
			}
			if (sb.length() > 0) {
				result.set(sb.substring(0, sb.length() - 1).toString());
				context.write(key, result);
			}
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: MutualFriends <input file path> <output file path>");
			System.exit(2);
		}

		// Configuring hadoop mapReduce job
		Job mapReduceJob = Job.getInstance(conf, "mutualFriends");
		mapReduceJob.setJarByClass(MutualFriends.class);
		mapReduceJob.setMapperClass(MutualFriends.Map.class);
		mapReduceJob.setReducerClass(MutualFriends.Reduce.class);
		// set output key type
		mapReduceJob.setOutputKeyClass(Text.class);
		// set output value type
		mapReduceJob.setOutputValueClass(Text.class);
		// setting hdfs input path for incoming file
		FileInputFormat.addInputPath(mapReduceJob, new Path(otherArgs[0]));
		// setting hdfs output path for storing generated output
		FileOutputFormat.setOutputPath(mapReduceJob, new Path(otherArgs[1]));
		System.exit(mapReduceJob.waitForCompletion(true) ? 0 : 1);

	}
}