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
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.io.*;
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




public class TTests {

	public TTests() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) throws IOException {
		
		
		
		
		
		//String sourceFile = "/user/ruslan/ruslan/intervals.txt";
		String sourceFile = "/user/ruslan/ruslan/intervals.txt.short";
		String indexFile = "/user/ruslan/ruslan/interval_index";
		
		Configuration config = new Configuration();
		config.addResource(new Path("/home/arslan/jeclipse/eclipse/workspace/hadoop_test/global_conf/core-site.xml"));
		config.addResource(new Path("/home/arslan/jeclipse/eclipse/workspace/hadoop_test/global_conf/hdfs-site.xml"));
		config.addResource(new Path("/home/arslan/jeclipse/eclipse/workspace/hadoop_test/global_conf/mapred-site.xml"));
		config.addResource(new Path("/home/arslan/jeclipse/eclipse/workspace/hadoop_test/global_conf/yarn-site.xml"));
		FileSystem fs = FileSystem.get(config);
		
		TSpatialHadoop spatialHadoop = new TSpatialHadoop(fs, config);
		//spatialHadoop.Convert2MBR(sourceFile, sourceFile + ".spatial_hadoop");
		
		
		
		/*
		//TDFSIntervaIndex index = new TDFSIntervaIndex(sourceFile, indexFile, fs, config);
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
