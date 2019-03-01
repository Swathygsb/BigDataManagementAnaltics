import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FriendDetails {

	// mapper class
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		// in memory storage for storing user details
		HashMap<String, String> idDetailsMap = new HashMap<String, String>();

		// Map output key and value declaration
		private Text friendsIdPair = new Text();
		private Text nameState = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] fileLine = value.toString().split("\t");
			if (fileLine.length == 2) {
				String mainUser = fileLine[0];
				String nameStateList = "";
				String[] friends = fileLine[1].split(",");
				for (String friendId : friends) {
					nameStateList += idDetailsMap.get(friendId) + ",";
				}
				nameState.set(nameStateList);
				for (String friendId : friends) {
					String pair = "";
					if (Integer.parseInt(mainUser) > Integer.parseInt(friendId))
						pair = friendId + "," + mainUser;
					else
						pair = mainUser + "," + friendId;
					friendsIdPair.set(pair);
					context.write(friendsIdPair, nameState);
				}
			}
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			// getting local of user details file
			Path filePath = new Path(conf.get("userDetailsFile"));
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(filePath);
			for (FileStatus status : fss) {
				Path path = status.getPath();
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
				String line = br.readLine();
				while (line != null) {
					String[] arr = line.split(",");
					// inmemory storage for user details - userid<key> , name:city <value>
					idDetailsMap.put(arr[0], arr[0] + "=" + arr[1] + ":" + arr[5]);
					line = br.readLine();
				}
			}
		}
	}

	// reducer class
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			HashSet<String> friendsList1 = new HashSet<>();
			HashSet<String> friendsList2 = new HashSet<>();
			HashSet<String> mutualFrnds = new HashSet<>();

			int index = 1;
			for (Text val : values) {
				if (index == 1) {
					friendsList1.addAll(Arrays.asList(val.toString().split(",")));

				} else if (index == 2) {
					friendsList2.addAll(Arrays.asList(val.toString().split(",")));
				}
				index++;
			}

			for (String f1 : friendsList1) {
				if (friendsList2.contains(f1)) {
					mutualFrnds.add(f1.split("=")[1]);
				}
			}

			StringBuilder sb = new StringBuilder();
			for (String s : mutualFrnds) {
				sb.append(s).append(",");
			}

			if (sb.length() > 0) {
				result.set("[" + sb.substring(0, sb.length() - 1).toString() + "]");
				if ((context.getConfiguration().get("givenpair")).equals(key.toString())) {
					context.write(key, result);
				}
			}
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		// getting command line arguments
		if (otherArgs.length != 5) {
			System.err.println(
					"Usage: FriendDetails <input-userDetails> <input-idFriendsIdList> <outputFilePath> userid1 userid2");
			System.exit(2);
		}

		conf.set("userDetailsFile", otherArgs[0]);
		int id1 = Integer.parseInt(otherArgs[3]);
		int id2 = Integer.parseInt(otherArgs[4]);
		String idpair = "";
		if (id1 > id2) {
			idpair = id2 + "," + id1;
		} else {
			idpair = id1 + "," + id2;
		}
		conf.set("givenpair", idpair);

		// configuring hadoop map reduce job
		Job mapReduceJob = Job.getInstance(conf, "friendDetails");
		mapReduceJob.setJarByClass(FriendDetails.class);
		mapReduceJob.setMapperClass(Map.class);
		mapReduceJob.setReducerClass(Reduce.class);
		mapReduceJob.setOutputKeyClass(Text.class);
		mapReduceJob.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(mapReduceJob, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(mapReduceJob, new Path(otherArgs[2]));
		System.exit(mapReduceJob.waitForCompletion(true) ? 0 : 1);
	}
}
