import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.time.Period;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FriendsAverageAge {

	public static class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {

		// in memory storage for storing user details
		HashMap<String, String> idDobMap = new HashMap<String, String>();

		// Map output key and value declaration
		private Text avgAge = new Text();
		private Text id = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String eachLine[] = value.toString().split("\t");
			if (eachLine.length == 2) {
				id.set(eachLine[0]);
				String fdIds[] = eachLine[1].split(",");
				int sum = 0, count = 0;
				for (String fid : fdIds) {
					String dob[] = idDobMap.get(fid).split("/");
					int age = Period.between(
							LocalDate.of(Integer.parseInt(dob[2]), Integer.parseInt(dob[0]), Integer.parseInt(dob[1])),
							LocalDate.now()).getYears();
					sum += age;
					count++;
				}
				int avgAgeInt = sum / count;
				avgAge.set(Integer.toString(avgAgeInt));
				context.write(id, avgAge);
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			// getting local of user details file
			Path filePath = new Path(conf.get("userDetailsFilePath"));
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(filePath);
			for (FileStatus status : fss) {
				Path path = status.getPath();
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
				String line = br.readLine();
				while (line != null) {
					String[] arr = line.split(",");
					// inmemory storage for user details - userid<key> , name:city <value>
					idDobMap.put(arr[0], arr[9]);
					line = br.readLine();
				}
			}
		}
	}

	public static class Mapper2 extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] info = value.toString().split(",");
			context.write(new Text(info[0]),
					new Text("info=" + info[3] + "," + info[4] + "," + info[5] + "," + info[6] + "," + info[7]));

		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Text result = new Text();
			Text age = new Text();
			String[] ids = null;
			for (Text value : values) {
				if (value.toString().contains("info=")) {
					String[] details = value.toString().split("=");
					result.set(details[1]);
				} else {
					age.set(value);
				}
			}

			context.write(new Text(key + "," + age), new Text(result.toString()));

		}
	}

	public static class MapTwo extends Mapper<Text, Text, LongWritable, Text> {
		LongWritable lg = new LongWritable();

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String[] info = key.toString().split(",");
			if (info.length == 2) {
				Long avgAge = Long.parseLong(info[1].trim());
				lg.set(avgAge);
				context.write(lg, new Text(info[0] + "=" + value));
			}
		}
	}

	public static class ReduceTwo extends Reducer<LongWritable, Text, Text, Text> {

		private int count = 0;

		public void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for (Text value : values) {
				String[] line = value.toString().split("=");
				if (count < 15) {
					context.write(new Text(line[0] + "," + line[1] + ", Avg age of direct friends: " + key), null);
					count++;
				}
			}

		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("userDetailsFilePath", otherArgs[1]);
		// get all args
		if (otherArgs.length != 4) {
			System.err.println("Usage: MutualFriends reduce side join <in> <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "FriendsAverageAge");

		job.setJarByClass(FriendsAverageAge.class);
		job.setReducerClass(FriendsAverageAge.Reduce.class);

		// Multiple mapper as inputs to Reducer
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, Mapper1.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, Mapper2.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);
		// set the HDFS path for the output
		Path outputPath = new Path(otherArgs[2]);
		FileOutputFormat.setOutputPath(job, outputPath);
		if (!job.waitForCompletion(true)) {
			System.exit(1);
		}

		else

		{
			// second job details
			conf = new Configuration();
			Job job2 = Job.getInstance(conf, "FriendsAvgAge2");
			FileInputFormat.addInputPath(job2, new Path(otherArgs[2]));

			job2.setInputFormatClass(KeyValueTextInputFormat.class);
			job2.setJarByClass(FriendsAverageAge.class);
			job2.setMapperClass(FriendsAverageAge.MapTwo.class);
			job2.setReducerClass(FriendsAverageAge.ReduceTwo.class);
			job2.setMapOutputKeyClass(LongWritable.class);
			job2.setMapOutputValueClass(Text.class);

			// set job2's output key type
			job2.setOutputKeyClass(Text.class);
			// set job2's output value type
			job2.setOutputValueClass(Text.class);

			FileOutputFormat.setOutputPath(job2, new Path(otherArgs[3]));

			if (!job2.waitForCompletion(true))
				System.exit(1);
			else
				System.exit(0);
		}

	}
}