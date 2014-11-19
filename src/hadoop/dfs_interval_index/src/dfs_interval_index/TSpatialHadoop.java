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
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.GridInfo;
import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;
import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.RandomSpatialGenerator;
import edu.umn.cs.spatialHadoop.ReadFile;
import edu.umn.cs.spatialHadoop.mapred.GridOutputFormat;
import edu.umn.cs.spatialHadoop.mapred.RandomInputFormat;
import edu.umn.cs.spatialHadoop.nasa.HDFPlot;
import edu.umn.cs.spatialHadoop.nasa.HDFToText;
import edu.umn.cs.spatialHadoop.nasa.MakeHDFVideo;
import edu.umn.cs.spatialHadoop.operations.RangeQuery;
import edu.umn.cs.spatialHadoop.operations.Repartition;
import edu.umn.cs.spatialHadoop.operations.Repartition.RepartitionReduce;



public class TSpatialHadoop {
	
	private FileSystem HDFS;
	private Configuration Config;

	public TSpatialHadoop(FileSystem hdfs, Configuration config) {
		HDFS = hdfs;
		Config = config;
	}
	
	public void CreateIndex(String inFile, String outFile) throws IOException {	
		OperationsParams params = new OperationsParams(Config);
		params.set("sindex", "rtree");
		params.set("shape", "rect");
		params.setBoolean("overwrite", true);
		params.setBoolean("local", false);
		Path InPath = new Path(inFile);
		Path OutPath = new Path(outFile);
		HDFS.delete(OutPath, true);
		
		try {
		
			Repartition.repartition(InPath, OutPath, params);
		} catch (java.io.IOException err) {
			System.out.println("Stupid mistake!");
			System.err.println(err.getMessage());
		}
	}
	
	public long Query(double queryLeft, double queryRight, String indexPath) throws IOException {
		OperationsParams params = new OperationsParams(Config, String.format("rect:%f,0,%f,1", queryLeft, queryRight));
		params.set("sindex", "grid");
		params.set("shape", "rect");
		params.setBoolean("overwrite", true);
		params.setBoolean("local", false);
		Path InPath = new Path(indexPath);
		Path OutPath = new Path("/user/ruslan/ruslan/sp_results");
		HDFS.delete(OutPath, true);
		return RangeQuery.rangeQuery(InPath, OutPath, new Rectangle(queryLeft, 0, queryRight, 1), params);
	}
	
	
	public void Convert2MBR(String InPath, String OutPath) throws IOException {
	
		JobConf job = new JobConf(Config);
		job.setJobName("Convertor");
		
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(GridOutputFormat.class);
		//Shape s = new Rectangle();
		
		job.setMapperClass(dfs_interval_index.TSpatialHadoop.Map.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Shape.class);
		ClusterStatus clusterStatus = new JobClient(job).getClusterStatus();
		//job.setNumMapTasks(10 * Math.max(1, clusterStatus.getMaxMapTasks()));
		//System.out.println(String.format("map tasks count: %d", clusterStatus.getMaxMapTasks()));
		job.setNumMapTasks(1);

		//String sindex = null;//params.get("sindex");
		//TODO: set as a paarameter
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
		//TODO: as a parameter
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
