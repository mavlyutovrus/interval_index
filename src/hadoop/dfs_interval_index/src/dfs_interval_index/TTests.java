package dfs_interval_index;


import dfs_interval_index.TDFSIntervaIndex;
import dfs_interval_index.TMapReduceSearcher;
import dfs_interval_index.TSpatialHadoop;

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
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.io.*;
import org.apache.hadoop.thriftfs.api.DatanodeInfo;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.MapFile.*;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ProgramDriver;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;



public class TTests {

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
		
		
		
		
		
		
		
/*		
		{

			ArrayList<Long> sizes = new ArrayList<Long>();
			for (int size = 1; size <= 65536 * 8; size = size << 1) {
				for (int attempt = 0; attempt < 1000; ++attempt) {
					sizes.add(new Long(size));					
				}
			}
			{
				long seed = System.nanoTime();
				Collections.shuffle(sizes, new Random(seed));
			}	
			
			String fileName = "/user/ruslan/ruslan/intervals.txt";
			final long fileLength = fs.getContentSummary(new Path(fileName)).getLength();
			FSDataInputStream file = fs.open(new Path(fileName));
			//warmup
			System.out.println(file.read());
			Random randomizer = new Random(System.nanoTime());
			for (int readIndex = 0; readIndex < sizes.size(); ++readIndex) {
				long chunkLength = sizes.get(readIndex);
				double random = randomizer.nextDouble();
				long offset = (long)(random * (double)(fileLength - chunkLength));
				byte[] buffer = new byte[(int)chunkLength];		
				{
					long start_time = System.currentTimeMillis();
					file.readFully(offset, buffer);
					long delta = System.currentTimeMillis() - start_time;
					System.out.println(String.format("%d\t%d\t%d\n", offset, chunkLength, delta));
				}
			}
		}*/
		
		
				
		/*
		FSDataInputStream file = fs.open(new Path(args[0]));
		byte[] buffer = new byte[Integer.parseInt(args[2])];
		final int offset = Integer.parseInt(args[1]);
		long start_time = System.currentTimeMillis();
		file.readFully(offset, buffer);
		System.out.println(System.currentTimeMillis() - start_time);
		*/
		
		
		/*
		for (int endOffset = 0; endOffset < 65536 * 3; endOffset += 65536) {
			for (int size = 1; size <= 65536; size *= 2) {
				byte[] buffer = new byte[size + endOffset];
				
				long start_time = System.currentTimeMillis();
				file.readFully(0, buffer);
				System.out.println(System.currentTimeMillis() - start_time);

				start_time = System.currentTimeMillis();
				file.readFully(65536, buffer);
				System.out.println(System.currentTimeMillis() - start_time);
				
				start_time = System.currentTimeMillis();
				file.readFully(65536 * 10, buffer);
				System.out.println(System.currentTimeMillis() - start_time);
				
				break;
			}
			break;
		}
		*/
		return;
		
		
		
		//String sourceFileMBR = sourceFile + ".spatial_hadoop";
		//String SHIndexFile = sourceFile + ".saptial_hadoop_index";
		
		
		/*
		TSpatialHadoop spatialHadoop = new TSpatialHadoop(fs, config);
		spatialHadoop.Convert2MBR(sourceFile, sourceFileMBR);
		spatialHadoop.CreateIndex(sourceFileMBR, SHIndexFile);
		{
			long start = System.currentTimeMillis();		
			long response = spatialHadoop.Query(0, 1000, SHIndexFile);
			System.out.println(String.format("Response size: %d", response));
			System.out.println(String.format("time: %d", System.currentTimeMillis() - start));
		}
		{
			long start = System.currentTimeMillis();
			TMapReduceSearcher MRSearch = new TMapReduceSearcher(sourceFile, fs, config);
			long numberOfResults = MRSearch.Search(0, 1000);
			System.out.println(String.format(".. test case %d", numberOfResults));
			System.out.println(String.format("time: %d", System.currentTimeMillis() - start));
		}
		*/
		
		
		//TDFSIntervaIndex index = new TDFSIntervaIndex(sourceFile, indexFile, fs, config);
		/*
		TDFSIntervaIndex index = new TDFSIntervaIndex(indexFile, fs, config);
		Random rand = new Random();
		for (int testCase = 0; testCase < 100000; ++testCase) {
			double queryStart =  rand.nextDouble() * (5000000);
			double queryEnd =   queryStart + rand.nextDouble() * 10000;
			long numberOfIndexResults = index.Search(queryStart, queryEnd);
			TMapReduceSearcher MRSearch = new TMapReduceSearcher(sourceFile, fs, config);
			long numberOfResults = MRSearch.Search(queryStart, queryEnd);
			if (numberOfResults != numberOfIndexResults) {
				System.err.println(String.format("ERRor %f-%f: %d != %d", queryStart, queryEnd, numberOfResults, numberOfIndexResults));
				System.exit(1);
			}
			System.out.println(String.format(".. test case %d", testCase));
		}
		*/
		
	}

}
