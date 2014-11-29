package dfs_interval_index;


import dfs_interval_index.TDFSIntervaIndex;
import dfs_interval_index.TMapReduceSearcher;
import dfs_interval_index.TSpatialHadoop;
import edu.umn.cs.spatialHadoop.core.CellInfo;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.SpatialSite;
import edu.umn.cs.spatialHadoop.mapred.GridOutputFormat;

import java.io.*;
import java.util.*;
import java.net.*;
import java.nio.*;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.io.*;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.MapFile.*;
import org.apache.hadoop.io.compress.SplittableCompressionCodec.READ_MODE;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ProgramDriver;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

import edu.umn.cs.spatialHadoop.core.Rectangle;





public class TTests {
	
	void read_test(String[] args) throws IOException {
		/*
		args = new String[2];
		args[0] = "/user/ruslan/ruslan/intervals.txt";
		args[1] = "10";
		*/
		String sourceFile = "/user/ruslan/ruslan/intervals.txt";
		//String sourceFile = "/user/ruslan/ruslan/intervals.txt.short";
		String indexFile = "/user/ruslan/ruslan/interval_index";
		
		Configuration config = new Configuration();
		config.addResource(new Path("/home/arslan/jeclipse/eclipse/workspace/hadoop_test/global_conf/core-site.xml"));
		config.addResource(new Path("/home/arslan/jeclipse/eclipse/workspace/hadoop_test/global_conf/hdfs-site.xml"));
		config.addResource(new Path("/home/arslan/jeclipse/eclipse/workspace/hadoop_test/global_conf/mapred-site.xml"));
		config.addResource(new Path("/home/arslan/jeclipse/eclipse/workspace/hadoop_test/global_conf/yarn-site.xml"));
		FileSystem fs = FileSystem.get(config);
		//DFSInputStream file = new DFSInputStream();
		
		//String fileName = "/user/ruslan/ruslan/intervals.txt";

	/*		args = new String[2];
		args[0] = "/user/ruslan/ruslan/intervals.txt";
		args[1] = "10";*/
		
		String fileName = args[0];
		final long fileLength = fs.getContentSummary(new Path(fileName)).getLength();
		HdfsDataInputStream file = (HdfsDataInputStream)fs.open(new Path(fileName));
		
		ArrayList<LocatedBlock> notLocalBlocks = new ArrayList<LocatedBlock>();
		{
			
			List<LocatedBlock> blocks = file.getAllBlocks();
			Iterator<LocatedBlock> it = blocks.iterator();
			while (it.hasNext()) {
				LocatedBlock block = it.next();
				long size = block.getBlockSize();
				long offset = block.getStartOffset();
				boolean onLocalMachine = false;
				for (int locIndex = 0; locIndex < block.getLocations().length; ++locIndex) {
					if (block.getLocations()[locIndex].getHostName().contains("diufpc301")) {
						onLocalMachine = true;
						break;
					} else {
						//System.out.println(block.getLocations()[locIndex].getCacheCapacity());
					}
				}
				if (!onLocalMachine) {
					notLocalBlocks.add(block);
				}
			}
			//last block is usually not full
			notLocalBlocks.remove(notLocalBlocks.size() - 1);
			System.out.println(String.format("blocks %d %d", blocks.size(), notLocalBlocks.size()));
		}
		
		System.out.flush();
		//warmup
		System.out.println(file.read());

		long[] sizes = new long[notLocalBlocks.size()];
		long[] offsets = new long[notLocalBlocks.size()];
		long[] inBlockOffset = new long[notLocalBlocks.size()]; 
		for (int blockIndex = 0; blockIndex < notLocalBlocks.size(); ++blockIndex) {
			long size = notLocalBlocks.get(blockIndex).getBlockSize();
			long offset = notLocalBlocks.get(blockIndex).getStartOffset();
			sizes[blockIndex] = size;
			offsets[blockIndex] = offset;
			inBlockOffset[blockIndex] = 0;
		}
		System.out.println(sizes[0]);
		int measuresCount = 493;
		//for (int readIndex = 0; readIndex < measuresCount; ++readIndex) 
		for (int attempt = 0; attempt < 2; ++attempt)
		{
			int readIndex = Integer.parseInt(args[1]);
			long readSize = (readIndex + 1) * 512;
			byte[] buffer = new byte[(int)readSize];
			long[] deltas = new long[notLocalBlocks.size()];
			long start_time = System.nanoTime();
			for (int blockIndex = 0; blockIndex < notLocalBlocks.size(); ++blockIndex) {
				long readOffset = offsets[blockIndex] + inBlockOffset[blockIndex];
				long one_start_time = System.nanoTime();
				file.readFully(readOffset, buffer);
				long one_delta = System.nanoTime() - one_start_time;
				deltas[blockIndex] = one_delta;				
				inBlockOffset[blockIndex] += readSize + 65536;
				//if (inBlockOffset[blockIndex] > sizes[blockIndex]) {
				//	System.out.println("fuck");
				//}
			}
			long delta = System.nanoTime() - start_time;
			
			if (attempt > 0) 
			{
				System.out.print(String.format("%d\t%d\t%d\t", readSize, notLocalBlocks.size(), delta));
				for (int blockIndex = 0; blockIndex < notLocalBlocks.size(); ++blockIndex) {
					System.out.print(String.format("%d;", deltas[blockIndex]));
				}
				System.out.println();
			}
		}
	}

	public TTests() {
		// TODO Auto-generated constructor stub
	}
	
	
	final static double MAX_START = 10000000000.0;
	final static double MIN_START = 0.0;	
	
	
	public static Path GenerateDataset(FileSystem HDFS, final long size, final int avgOverlapping) throws IOException {
		double intervalCell = (MAX_START - MIN_START) / size;
		double optimalLength = intervalCell * avgOverlapping + 0.001;
		String filename = String.format("/user/ruslan/ruslan/intervals_%d_%d.txt", size, avgOverlapping);
		HDFS.delete(new Path(filename), true);
		FSDataOutputStream file = HDFS.create(new Path(filename));
		Random randomizer = new Random(0);
		for (long index = 0; index < size; ++index) {
			double start = randomizer.nextDouble() * (MAX_START - MIN_START) + MIN_START;
			file.writeBytes(String.format("%f %f %d\n", start, optimalLength, index + 1));
		}
		file.close();
		return new Path(filename);
		
	}
	

	public static void main(String[] args) throws IOException {
		
		String sourceFile = "/user/ruslan/ruslan/intervals.txt";
		//String sourceFile = "/user/ruslan/ruslan/intervals.txt.short";
		String indexFile = "/user/ruslan/ruslan/interval_index";
		
		Configuration config = new Configuration();
		config.addResource(new Path("/home/arslan/jeclipse/eclipse/workspace/hadoop_test/global_conf/core-site.xml"));
		config.addResource(new Path("/home/arslan/jeclipse/eclipse/workspace/hadoop_test/global_conf/hdfs-site.xml"));
		config.addResource(new Path("/home/arslan/jeclipse/eclipse/workspace/hadoop_test/global_conf/mapred-site.xml"));
		config.addResource(new Path("/home/arslan/jeclipse/eclipse/workspace/hadoop_test/global_conf/yarn-site.xml"));
		config.addResource(new Path("core-site.xml"));
		config.addResource(new Path("hdfs-site.xml"));
		config.addResource(new Path("mapred-site.xml"));
		config.addResource(new Path("yarn-site.xml"));
		FileSystem fs = FileSystem.get(config);
		
		/*
		HdfsDataInputStream IndexFile = (HdfsDataInputStream)fs.open(new Path("/user/ruslan/ruslan/intervals_1000000000_100.txt"));
		List<LocatedBlock> blocks = IndexFile.getAllBlocks(); 
		int count = 0;
		int blockIndex = 0;
		for (LocatedBlock block : blocks) {
			for (DatanodeInfo info :block.getLocations()) {
				if (info.getHostName().contains("301")) {
					System.out.println(info.getHostName());
				}
			}
		}
		*/
		
		
		
		long[] datasetSizes = {(long)Math.pow(10, 7), (long)Math.pow(10, 8), (long)Math.pow(10, 9)};
		int[] avgOverlappings = {10, 100, 10000};
		
		/*
		//long[] datasetSizes = {(long)Math.pow(10, 7), (long)Math.pow(10, 8)};
		//int[] avgOverlappings = {10, 100, 10000};

		//long[] datasetSizes = {(long)Math.pow(10, 9)};
		//int[] avgOverlappings = {100};
		
		//create datasets
		for (int sizeIndex = 0; sizeIndex < datasetSizes.length; ++sizeIndex) {
			for (int overlappingIndex = 0; overlappingIndex < avgOverlappings.length; ++overlappingIndex) {
				final long datasetSize = datasetSizes[sizeIndex];
				final int avgOverlapping = avgOverlappings[overlappingIndex];
				System.out.println(datasetSize);
				Path filePath = GenerateDataset(fs, datasetSize, avgOverlapping);
				System.out.println(filePath.toString());
			}
		}	
		//create ii	
		for (int sizeIndex = 0; sizeIndex < datasetSizes.length; ++sizeIndex) {
			for (int overlappingIndex = 0; overlappingIndex < avgOverlappings.length; ++overlappingIndex) {
				final long datasetSize = datasetSizes[sizeIndex];
				final int avgOverlapping = avgOverlappings[overlappingIndex];
				String filename = String.format("/user/ruslan/ruslan/intervals_%d_%d.txt", datasetSize, avgOverlapping);
				long time = System.currentTimeMillis();
				TDFSIntervaIndex index = new TDFSIntervaIndex(filename, filename + ".index", fs, config);
				long delta = System.currentTimeMillis() - time;
				System.out.println(String.format("CONST_TIME %s %d", filename + ".index", delta));
			}
		}		
		//create spatial hadoop
		for (int sizeIndex = 0; sizeIndex < datasetSizes.length; ++sizeIndex) {
			for (int overlappingIndex = 0; overlappingIndex < avgOverlappings.length; ++overlappingIndex) {
				final long datasetSize = datasetSizes[sizeIndex];
				final int avgOverlapping = avgOverlappings[overlappingIndex];
				String filename = String.format("/user/ruslan/ruslan/intervals_%d_%d.txt", datasetSize, avgOverlapping);
				long time = System.currentTimeMillis();
				{
					TSpatialHadoop spatialHadoop = new TSpatialHadoop(fs, config);
					spatialHadoop.Convert2MBR(0, MAX_START + 20000, filename, filename + ".mbr");
					spatialHadoop.CreateIndex(filename + ".mbr", filename + ".shindex", "r+tree");
					fs.delete(new Path(filename + ".mbr"), true);
				}
				long delta = System.currentTimeMillis() - time;
				System.out.println(String.format("CONST_TIME %s %d", filename + ".shindex", delta));
			}
		}	
		*/

		//query performance
		{
			Random randomizer = new Random(0);
			for (int sizeIndex = 0; sizeIndex < datasetSizes.length; ++sizeIndex) {
				for (int overlappingIndex = 0; overlappingIndex < avgOverlappings.length; ++overlappingIndex) {
					final long datasetSize = datasetSizes[sizeIndex];
					final int avgOverlapping = avgOverlappings[overlappingIndex];
					if (datasetSize == (long)Math.pow(10, 9) && avgOverlapping != 100) {
						continue;
					}
					if (datasetSize != (long)Math.pow(10, 9)) {
						continue;
					}
					
					String filename = String.format("/user/ruslan/ruslan/intervals_%d_%d.txt", datasetSize, avgOverlapping);
					System.out.println("RESPONSE for " + filename);
					TDFSIntervaIndex index = new TDFSIntervaIndex(filename + ".index", fs, config);
//					{//warmup
//						index.ReadsCount = 0;
//						double responseSize = 0;
//						long startTime = System.currentTimeMillis();
//						final int II_QUERIES = 1000000;
//						for (int queryIndex = 0; queryIndex < II_QUERIES; ++queryIndex) {
//							double queryStart = randomizer.nextDouble() * (MAX_START - MIN_START) + MIN_START;
//							responseSize += index.Search(queryStart, queryStart + 1000);
//						}
//						double timeDelta = (double)(System.currentTimeMillis() - startTime) / II_QUERIES;
//						responseSize = responseSize / II_QUERIES;
//						double readsCount = (double)index.ReadsCount / II_QUERIES;
//						//System.out.println(String.format("RESPONSE interval_index_cold read_count %f qlen %f response_size %f time %f", 
//						//												readsCount, queryLength, responseSize, timeDelta));						
//					}					
					double[] queryLengths = {10000, 100};
					for (int lengthIndex = 0; lengthIndex < queryLengths.length; ++lengthIndex) {
						double queryLength = queryLengths[lengthIndex];
						final int ATTEMPTS = 100;
						for (int attempt = 0; attempt < ATTEMPTS; ++attempt) {
//							{
//								index.ReadsCount = 0;
//								double responseSize = 0;
//								long startTime = System.currentTimeMillis();
//								final int II_QUERIES = 10;
//								for (int queryIndex = 0; queryIndex < II_QUERIES; ++queryIndex) {
//									double queryStart = randomizer.nextDouble() * (MAX_START - MIN_START) + MIN_START;
//									responseSize += index.Search(queryStart, queryStart + queryLength);
//								}
//								double timeDelta = (double)(System.currentTimeMillis() - startTime) / II_QUERIES;
//								responseSize = responseSize / II_QUERIES;
//								double readsCount = (double)index.ReadsCount / II_QUERIES;
//								System.out.println(String.format("RESPONSE interval_index_cold read_count %f qlen %f response_size %f time %f", 
//																				readsCount, queryLength, responseSize, timeDelta));						
//							}
//							{
//								TSpatialHadoop shIndex = new TSpatialHadoop(fs, config);
//								long startTime = System.currentTimeMillis();
//								double queryStart = randomizer.nextDouble() * (MAX_START - MIN_START) + MIN_START;
//								long responseSize = shIndex.Query(queryStart, queryStart + queryLength, filename + ".shindex", "r+tree");
//								long timeDelta = System.currentTimeMillis() - startTime;
//								System.out.println(String.format("RESPONSE spatial_hadoop qlen %f response_size %d time %d", 
//																						queryLength, responseSize, timeDelta));
//							}
//							
//							{
//								TMapReduceSearcher MapReduceSearch = new TMapReduceSearcher(filename, fs, config);
//								long startTime = System.currentTimeMillis();
//								double queryStart = randomizer.nextDouble() * (MAX_START - MIN_START) + MIN_START;
//								TInterval[] queries = {new TInterval(queryStart, queryStart + queryLength, 1)};
//								long responseSize = MapReduceSearch.Search(queries);
//								long timeDelta = System.currentTimeMillis() - startTime;
//								System.out.println(String.format("RESPONSE map_reduce qlen %f response_size %d time %d", 
//																						queryLength, responseSize, timeDelta));
//							}
						}
					}	
				}
			}
		}
		
		{//real dataset
			TInterval[] queries = {new TInterval(73427291.000000, 73427411.000000, 0),
					new TInterval(95571254.000000, 95571374.000000, 0),
					new TInterval(4045099.000000, 4045219.000000, 0),
					new TInterval(6703951.000000, 6704071.000000, 0),
					new TInterval(1080868.000000, 1080988.000000, 0),
					new TInterval(60781378.000000, 60781498.000000, 0),
					new TInterval(67049099.000000, 67049219.000000, 0),
					new TInterval(113236924.000000, 113237044.000000, 0),
					new TInterval(73536736.000000, 73536856.000000, 0),
					new TInterval(14840658.000000, 14840778.000000, 0),
					new TInterval(1010526.000000, 1010646.000000, 0),
					new TInterval(63411626.000000, 63411746.000000, 0),
					new TInterval(77320322.000000, 77320442.000000, 0),
					new TInterval(3143217.000000, 3143337.000000, 0),
					new TInterval(125507331.000000, 125507451.000000, 0),
					new TInterval(17035504.000000, 17035624.000000, 0),
					new TInterval(85342735.000000, 85342855.000000, 0),
					new TInterval(94337125.000000, 94337245.000000, 0),
					new TInterval(57573881.000000, 57574001.000000, 0),
					new TInterval(44616183.000000, 44616303.000000, 0),
					new TInterval(47440380.000000, 47440500.000000, 0),
					new TInterval(69962524.000000, 69962644.000000, 0),
					new TInterval(101852597.000000, 101852717.000000, 0),
					new TInterval(67010461.000000, 67010581.000000, 0),
					new TInterval(76250603.000000, 76250723.000000, 0),
					new TInterval(68455464.000000, 68455584.000000, 0),
					new TInterval(67374451.000000, 67374571.000000, 0),
					new TInterval(4410871.000000, 4410991.000000, 0),
					new TInterval(105010389.000000, 105010509.000000, 0),
					new TInterval(4308841.000000, 4308961.000000, 0) };
			
			final int MAX_BP_INDEX = 300000000;
			final String EXOME_DATASET = "/user/ruslan/ruslan/exome.txt";
			
//			{//construction
//				String filename = String.format(EXOME_DATASET);
//				{
//					long time = System.currentTimeMillis();
//					TDFSIntervaIndex index = new TDFSIntervaIndex(filename, filename + ".index", fs, config);
//					long delta = System.currentTimeMillis() - time;
//					System.out.println(String.format("CONST_TIME %s %d", filename + ".index", delta));		
//				}
//	
//				{
//					long time = System.currentTimeMillis();
//					TSpatialHadoop spatialHadoop = new TSpatialHadoop(fs, config);
//					spatialHadoop.Convert2MBR(0, MAX_BP_INDEX, filename, filename + ".mbr");
//					spatialHadoop.CreateIndex(filename + ".mbr", filename + ".shindex", "r+tree");
//					long delta = System.currentTimeMillis() - time;	
//					fs.delete(new Path(filename + ".mbr"), true);
//					System.out.println(String.format("CONST_TIME %s %d", filename + ".shindex", delta));				
//				}
//			}

			{//queries
				Random randomizer = new Random(0);
				TDFSIntervaIndex index = new TDFSIntervaIndex(EXOME_DATASET + ".index", fs, config);
//				{//warmup
//					
//					index.ReadsCount = 0;
//					double responseSize = 0;
//					long startTime = System.currentTimeMillis();
//					final int II_QUERIES = 100000;
//					for (int queryIndex = 0; queryIndex < II_QUERIES; ++queryIndex) {
//						double queryStart = randomizer.nextDouble() * MAX_BP_INDEX;
//						responseSize += index.Search(queryStart, queryStart + 1000);
//					}
//					double timeDelta = (double)(System.currentTimeMillis() - startTime) / II_QUERIES;
//					responseSize = responseSize / II_QUERIES;
//					double readsCount = (double)index.ReadsCount / II_QUERIES;
//					//System.out.println(String.format("RESPONSE interval_index_cold read_count %f qlen %f response_size %f time %f", 
//					//												readsCount, queryLength, responseSize, timeDelta));						
//				}
				final int ATTEMPTS = 30;
				for (int attempt = 0; attempt < ATTEMPTS; ++attempt) {
//					{
//						index.ReadsCount = 0;
//						double responseSize = 0;
//						long startTime = System.currentTimeMillis();
//						final int II_QUERIES = 1;
//						double queryStart = queries[attempt].Start;
//						double queryEnd = queries[attempt].End;
//						responseSize += index.Search(queryStart, queryEnd);
//						double timeDelta = (double)(System.currentTimeMillis() - startTime) / II_QUERIES;
//						responseSize = responseSize / II_QUERIES;
//						double readsCount = (double)index.ReadsCount / II_QUERIES;
//						System.out.println(String.format("RESPONSE interval_index_cold read_count %f qlen 120 response_size %f time %f", 
//																		readsCount, responseSize, timeDelta));						
//					}					
//					{
//						TSpatialHadoop shIndex = new TSpatialHadoop(fs, config);
//						long startTime = System.currentTimeMillis();
//						long responseSize = shIndex.Query(queries[attempt].Start, queries[attempt].End, EXOME_DATASET + ".shindex", "r+tree");
//						long timeDelta = System.currentTimeMillis() - startTime;
//						System.out.println(String.format("RESPONSE spatial_hadoop qlen 120 response_size %d time %d", 
//																				responseSize, timeDelta));
//					}
//					{
//						TMapReduceSearcher MapReduceSearch = new TMapReduceSearcher(EXOME_DATASET, fs, config);
//						long startTime = System.currentTimeMillis();
//						TInterval[] query = {queries[attempt]};
//						long responseSize = MapReduceSearch.Search(query);
//						long timeDelta = System.currentTimeMillis() - startTime;
//						System.out.println(String.format("RESPONSE map_reduce qlen 120 response_size %d time %d", 
//																				responseSize, timeDelta));
//					}
				}				
			}
		}

	}

}
