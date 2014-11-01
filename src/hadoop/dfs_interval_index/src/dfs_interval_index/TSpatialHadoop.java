package dfs_interval_index;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.GridInfo;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;
import edu.umn.cs.spatialHadoop.RandomSpatialGenerator;
import edu.umn.cs.spatialHadoop.ReadFile;
import edu.umn.cs.spatialHadoop.mapred.GridOutputFormat;
import edu.umn.cs.spatialHadoop.mapred.RandomInputFormat;
import edu.umn.cs.spatialHadoop.nasa.HDFPlot;
import edu.umn.cs.spatialHadoop.nasa.HDFToText;
import edu.umn.cs.spatialHadoop.nasa.MakeHDFVideo;
import edu.umn.cs.spatialHadoop.operations.Repartition;
import edu.umn.cs.spatialHadoop.operations.Repartition.RepartitionReduce;



public class TSpatialHadoop {
	
	private FileSystem HDFS;
	private Configuration Config;

	public TSpatialHadoop(FileSystem hdfs, Configuration config) {
		HDFS = hdfs;
		Config = config;
	}
	
	public void Convert2MBR(String InPath, String OutPath) throws IOException {
	
		JobConf job = new JobConf(Config);
		job.setJobName("Generator");
		Shape shape = new Rectangle(); //params.getShape("shape");

		ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();

		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(GridOutputFormat.class);
		
		job.setMapperClass(dfs_interval_index.TSpatialHadoop.Map.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(shape.getClass());
		job.setNumMapTasks(10 * Math.max(1, clusterStatus.getMaxMapTasks()));

		String sindex = null;//params.get("sindex");
		Rectangle mbr = new Rectangle(0, 0, 5002000, 1);

		CellInfo[] cells;
		cells = new CellInfo[] { new CellInfo(1, mbr) };


		SpatialSite.setCells(job, cells);

		// Do not set a reduce function. Use the default identity reduce
		// function
		if (cells.length == 1) {
			// All objects are in one partition. No need for a reduce phase
			job.setNumReduceTasks(0);
		} else {
			// More than one partition. Need a reduce phase to group shapes of
			// the
			// same partition together
			job.setReducerClass(RepartitionReduce.class);
			job.setNumReduceTasks(Math.max(1, Math.min(cells.length,
					(clusterStatus.getMaxReduceTasks() * 9 + 5) / 10)));
		}

		// Set paths
		//job.setJar("/home/arslan/src/spatialhadoop-2.2/lib/spatialhadoop-2.2.jar");
		job.setJar("/home/arslan/src/1d_interval_index/src/hadoop/dfs_interval_index.jar");
		FileInputFormat.setInputPaths(job, new Path(InPath));
		HDFS.delete(new Path(OutPath), true);
		FileOutputFormat.setOutputPath(job, new Path(OutPath));
		
		JobClient.runJob(job);

		FileStatus[] resultFiles = HDFS.listStatus(new Path(OutPath), new PathFilter() {
			@Override
			public boolean accept(Path path) {
				return path.getName().contains("_master");
			}
		});
		String ext = resultFiles[0].getPath().getName()
				.substring(resultFiles[0].getPath().getName().lastIndexOf('.'));
		Path masterPath = new Path(new Path(OutPath), "_master" + ext);
		
		OutputStream destOut = HDFS.create(masterPath);
		byte[] buffer = new byte[4096];
		for (FileStatus f : resultFiles) {
			InputStream in = HDFS.open(f.getPath());
			int bytes_read;
			do {
				bytes_read = in.read(buffer);
				if (bytes_read > 0)
					destOut.write(buffer, 0, bytes_read);
			} while (bytes_read > 0);
			in.close();
			HDFS.delete(f.getPath(), false);
		}
		destOut.close();

	}

	public static class Map extends MapReduceBase
								implements Mapper<LongWritable, Text, IntWritable, Rectangle> {
		/**List of cells used by the mapper*/
		private CellInfo[] cellInfos;

		/**Used to output intermediate records*/
		private IntWritable cellId = new IntWritable();

		@Override
		public void configure(JobConf job) {
			try {
				cellInfos = SpatialSite.getCells(job);
				super.configure(job);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, Rectangle> output, Reporter reporter)
						throws IOException {
			String[] chunks = value.toString().split(" ");
			Double start = Double.parseDouble(chunks[0]);
			Double end = start + Double.parseDouble(chunks[1]);
			Integer id = Integer.parseInt(chunks[2]);
			Rectangle shape = new Rectangle(start, 0, end, 1);
			for (int cellIndex = 0; cellIndex < cellInfos.length; cellIndex++) {
				if (cellInfos[cellIndex].isIntersected(shape.getMBR())) {
					cellId.set((int) cellInfos[cellIndex].cellId);
					output.collect(cellId, shape);
				}
			}
		}
	}

	
}
