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
import org.apache.hadoop.thriftfs.api.DatanodeInfo;
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

	public static void main(String[] args) throws IOException {
		
		String sourceFile = "/user/ruslan/ruslan/intervals.txt";
		//String sourceFile = "/user/ruslan/ruslan/intervals.txt.short";
		String indexFile = "/user/ruslan/ruslan/interval_index";
		
		Configuration config = new Configuration();
		config.addResource(new Path("/home/arslan/jeclipse/eclipse/workspace/hadoop_test/global_conf/core-site.xml"));
		config.addResource(new Path("/home/arslan/jeclipse/eclipse/workspace/hadoop_test/global_conf/hdfs-site.xml"));
		config.addResource(new Path("/home/arslan/jeclipse/eclipse/workspace/hadoop_test/global_conf/mapred-site.xml"));
		config.addResource(new Path("/home/arslan/jeclipse/eclipse/workspace/hadoop_test/global_conf/yarn-site.xml"));
		FileSystem fs = FileSystem.get(config);
		
		
		//TDFSIntervaIndex index = new TDFSIntervaIndex(sourceFile, indexFile, fs, config);
		
		TDFSIntervaIndex index = new TDFSIntervaIndex(indexFile, fs, config);
		int MAX_QUERIES = 100000;
		double MIN_VALUE = 0;
		double MAX_VALUE = 10000000000.0;
		double QUERY_LENGHT = 1000;
		ArrayList<Rectangle> allQueries = new ArrayList<Rectangle>();
		Random randomizer = new Random(0); //be deterministic
		for (int queryIndex = 0; queryIndex < MAX_QUERIES; ++queryIndex) {
			double queryStart = randomizer.nextDouble() * (MAX_VALUE - MIN_VALUE);
			double queryEnd = queryStart + QUERY_LENGHT;
			allQueries.add(new Rectangle(queryStart, 0, queryEnd, 1));
		}
		
   		ArrayList<Integer> queriesCountPerExperiment = new ArrayList<Integer>();
		{
			int queriesCount = 1;
			queriesCountPerExperiment.add(queriesCount);
			queriesCount = 10;
			int RUN_COUNT = 30;
			double factor = Math.exp(Math.log(MAX_QUERIES / (double)(queriesCount) ) / (RUN_COUNT - 1));
			for (; queriesCount <= MAX_QUERIES; queriesCount *= factor) {
				queriesCountPerExperiment.add((int)queriesCount);
			}
		}
		
		
		for (int dataSetIndex = 0; dataSetIndex < queriesCountPerExperiment.size(); ++dataSetIndex) {
			final int queriesCount = queriesCountPerExperiment.get(dataSetIndex);
			TInterval[] queries = new TInterval[queriesCount];
			for (int queryIndex = 0; queryIndex < queriesCount; ++queryIndex) {
				double queryStart = allQueries.get(queryIndex).x1;
				double queryEnd   = allQueries.get(queryIndex).x2;
				queries[queryIndex] = new TInterval(queryStart, queryEnd, queryIndex + 1);
			}
			
			TMapReduceSearcher MRSearch = new TMapReduceSearcher(sourceFile, fs, config);
			{
				long start = System.currentTimeMillis();
				long numberOfResults = MRSearch.Search(queries);
				final long time = System.currentTimeMillis() - start;
				System.out.println(String.format("mapreduce queries %d response %d time %d", queriesCount, numberOfResults, time));
			}
		}
		
		
		/*
		String sourceFileMBR = sourceFile + ".spatial_hadoop";
		String SHIndexFile = sourceFile + ".saptial_hadoop_index";		
		TSpatialHadoop spatialHadoop = new TSpatialHadoop(fs, config);
		
		for (int dataSetIndex = 0; dataSetIndex < queriesCountPerExperiment.size(); ++dataSetIndex) {
			final int queriesCount = queriesCountPerExperiment.get(dataSetIndex);
			CellInfo[] queries = new CellInfo[queriesCount];
			for (int queryIndex = 0; queryIndex < queriesCount; ++queryIndex) {
				double queryStart = allQueries.get(queryIndex).x1;
				double queryEnd   = allQueries.get(queryIndex).x2;
				queries[queryIndex] = new CellInfo(queryIndex + 1, new Rectangle(queryStart, 0, queryEnd, 1));
			}
			
			{
				long start = System.currentTimeMillis();		
				final long response = spatialHadoop.QueryRTree(queries, SHIndexFile + "_r+tree", "r+tree");
				final long time = System.currentTimeMillis() - start;
				System.out.println(String.format("spatial_hadoop queries %d response %d time %d", queriesCount, response, time));
			}
		}
		
		for (int readSize = 1024; readSize <= TDFSIntervaIndex.PAGE_SIZE; readSize *= 2)
		{
			int queriesPerExperimentPosition = 0;
			final long start = System.currentTimeMillis();
			long totalResults = 0;
			for (int queryIndex = 0; queryIndex < allQueries.size(); ++queryIndex) {
				totalResults += index.Search(allQueries.get(queryIndex).x1, allQueries.get(queryIndex).x2, readSize);
				boolean reportCase = false;
				if (queriesPerExperimentPosition < queriesCountPerExperiment.size()) {
					if (queriesCountPerExperiment.get(queriesPerExperimentPosition) == queryIndex + 1) {
						reportCase = true;
						++queriesPerExperimentPosition;
					}
				}
				if (queryIndex % 1000 == 0 || reportCase) {
					final long time = System.currentTimeMillis() - start;
					System.out.println(String.format("interval_index read_size %d queries %d response %d time %d", readSize, queryIndex + 1, totalResults, time));					
				}
			}
		}
		*/

		

		//spatialHadoop.Convert2MBR(0, 10000020000.0, sourceFile, sourceFileMBR);
		//spatialHadoop.CreateIndex(sourceFileMBR, SHIndexFile + "_rtree", "rtree");
		//spatialHadoop.CreateIndex(sourceFileMBR, SHIndexFile + "_r+tree", "r+tree");
		//spatialHadoop.CreateIndex(sourceFileMBR, SHIndexFile + "_grid", "grid");
		
		
		/*
		{
			long start = System.currentTimeMillis();		
			long response = spatialHadoop.Query(0, 1000, SHIndexFile + "_rtree", "rtree");
			System.out.println(String.format("response shadoop_rtree: %d", response));
			System.out.println(String.format("time: %d", System.currentTimeMillis() - start));
		}
		{
			long start = System.currentTimeMillis();		
			long response = spatialHadoop.Query(0, 1000, SHIndexFile + "_r+tree", "r+tree");
			System.out.println(String.format("response shadoop_r+tree: %d", response));
			System.out.println(String.format("time: %d", System.currentTimeMillis() - start));
		}
		{
			long start = System.currentTimeMillis();		
			long response = spatialHadoop.Query(0, 1000, SHIndexFile + "_grid", "grid");
			System.out.println(String.format("response shadoop_grid: %d", response));
			System.out.println(String.format("time: %d", System.currentTimeMillis() - start));
		}
		{
			long start = System.currentTimeMillis();
			TMapReduceSearcher MRSearch = new TMapReduceSearcher(sourceFile, fs, config);
			long numberOfResults = MRSearch.Search(0, 1000);
			System.out.println(String.format("response mapreduce: %d", numberOfResults));
			System.out.println(String.format("time: %d", System.currentTimeMillis() - start));
		}
		*/
				
		
		
		
		//TDFSIntervaIndex index = new TDFSIntervaIndex(sourceFile, indexFile, fs, config);
		
		/*
		TDFSIntervaIndex index = new TDFSIntervaIndex(indexFile, fs, config);
		Random rand = new Random();
		for (int testCase = 0; testCase < 100000; ++testCase) {
			long MAX = (long)1000000000 * 10;
			double queryStart =  rand.nextDouble() * MAX;
			double queryEnd =   queryStart + rand.nextDouble() * 10000;
			long numberOfIndexResults = index.Search(queryStart, queryEnd);
			TMapReduceSearcher MRSearch = new TMapReduceSearcher(sourceFile, fs, config);
			long numberOfResults = MRSearch.Search(queryStart, queryEnd);
			if (numberOfResults != numberOfIndexResults) {
				System.err.println(String.format("ERRor %f-%f: %d != %d", queryStart, queryEnd, numberOfResults, numberOfIndexResults));
				System.exit(1);
			}
			System.out.println(String.format(".. test case %d: [%f,%f], %d", testCase, queryStart, queryEnd, numberOfIndexResults));
		}
		*/
		
	
	}

}
