package dfs_interval_index;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

public class TMapReduceSearcher {
	
	private Path SourceFilePath;
	private FileSystem HDFS;
	private Configuration Config;

	public TMapReduceSearcher(String sourceFilePath, FileSystem hdfs, Configuration config) {
		SourceFilePath = new Path(sourceFilePath);
		HDFS = hdfs;
		Config = config;
	}
	
	public long Search(final double start, final double end) throws IOException {
		if (end <= start) {
			return 0;
		}
		
		
		
	
		JobConf jobConfig = new JobConf(Config);
		
		//TODO: replace to .sorted made of TIntervals
		FileInputFormat.setInputPaths(jobConfig, SourceFilePath);
		Path resultsFile = new Path(SourceFilePath + ".qres"); 
		HDFS.delete(resultsFile, true);
		FileOutputFormat.setOutputPath(jobConfig, resultsFile);		
		
		jobConfig.setJobName("search_request" + String.format("_%f_%f", start ,end));
		
		jobConfig.set("query_interval_start", String.valueOf(start));
		jobConfig.set("query_interval_end", String.valueOf(end));

		jobConfig.setMapOutputKeyClass(dfs_interval_index.TInterval.class);
		jobConfig.setMapOutputValueClass(NullWritable.class);			
		jobConfig.setOutputKeyClass(dfs_interval_index.TInterval.class);
		jobConfig.setOutputValueClass(NullWritable.class);

		jobConfig.setMapperClass(dfs_interval_index.TSearchMR.Map.class);
		jobConfig.setReducerClass(dfs_interval_index.TSearchMR.Reduce.class);
		//Turn-off reduce stage
		jobConfig.setNumReduceTasks(0);
		
		jobConfig.setInputFormat(TextInputFormat.class);
		jobConfig.setOutputFormat(SequenceFileOutputFormat.class);
		jobConfig.setQuietMode(true);
		
		jobConfig.setJar("/home/arslan/src/1d_interval_index/src/hadoop/dfs_interval_index.jar");


		try {
			JobClient.runJob(jobConfig);
		} catch ( java.lang.IllegalArgumentException exception) {
			System.out.println("stupid exception");
		}
		
		long numberOfResults = 0;
		FileStatus[] reduceFiles = HDFS.listStatus(new Path(resultsFile + "/"));
		for (FileStatus status : reduceFiles) {
	        Path path = status.getPath();
	        if (path.toString().endsWith("_SUCCESS")) {
	        	continue;
	        }
			SequenceFile.Reader reader = new SequenceFile.Reader(HDFS, path, Config);
			TInterval key = new TInterval();
			NullWritable nullWritable = NullWritable.get();	
			while (reader.next(key, nullWritable)) {
				++numberOfResults;
			}
	    }
		return numberOfResults;
	}	

}
