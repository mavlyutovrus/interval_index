package dfs_interval_index;

import dfs_interval_index.TInterval;
import dfs_interval_index.TSortMR;

import java.nio.ByteBuffer;
import java.io.IOException;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.PriorityQueue;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class TDFSIntervaIndex {
	
	private int CheckpointInterval = 201;
	public static final int PAGE_SIZE = 65536;
	public static final int MIN_BLOCK_SIZE = 512;
	private ArrayList<TSkipListElem> SkipList;
	private String IndexFilePath;
	FSDataInputStream IndexFile;
	FSDataInputStream CheckpointsFile;
	
	
	public TDFSIntervaIndex() {
		// TODO Auto-generated constructor stub
	}
	
	
	private class TRightBorderSorter implements Comparable<TRightBorderSorter> {
		public TInterval Interval;
		public TRightBorderSorter() {
		}
		public TRightBorderSorter(TInterval interval) {
			Interval = new TInterval(interval);
		}
		public boolean LessThan(TRightBorderSorter second) {
			if (Interval.End != second.Interval.End) {
				return Interval.End < second.Interval.End;
			} else {
				return Interval.LessThan(second.Interval);
			}
		}
		public boolean Equal(TRightBorderSorter second) {
			return Interval.Equal(second.Interval);
		}
		public int CompareValue(TRightBorderSorter second) {
			if (Equal(second)) {
				return 0;
			}
			if (LessThan(second)) {
				return -1;
			}
			return 1;
		}

		@Override
		public int compareTo(TRightBorderSorter second) {
			return CompareValue(second);
		}
	}
	
	private class TRightBorderReversedSorter  implements Comparable<TRightBorderReversedSorter> {
		public TInterval Interval;

		public TRightBorderReversedSorter(TInterval interval) {
			Interval = new TInterval(interval);
		}		
		
		public boolean LessThan(TRightBorderReversedSorter second) {
			if (Interval.End != second.Interval.End) {
				return Interval.End > second.Interval.End;
			} else {
				return !Interval.LessThan(second.Interval);
			}
		}
		public boolean Equal(TRightBorderReversedSorter second) {
			return Interval.Equal(second.Interval);
		}
		public int CompareValue(TRightBorderReversedSorter second) {
			if (Equal(second)) {
				return 0;
			}
			if (LessThan(second)) {
				return -1;
			}
			return 1;
		}
		@Override
		public int compareTo(TRightBorderReversedSorter second) {
			return CompareValue(second);
		}
	}
	
	private class TSkipListElem {
		long Offset;
		double MinLeftBorder;
		TSkipListElem(long offset, double minLeftBorder) {
			Offset = offset;
			MinLeftBorder = minLeftBorder;
		}
		
	}
	
	
	public TDFSIntervaIndex(String indexFilePath, FileSystem hdfs, Configuration config) throws IOException {
		IndexFilePath = indexFilePath;
		IndexFile = hdfs.open(new Path(IndexFilePath + ".index"));
		CheckpointsFile = hdfs.open(new Path(IndexFilePath + ".checkpoints"));	
		FSDataInputStream metaFile = hdfs.open(new Path(IndexFilePath + ".meta"));
		CheckpointInterval = (int)metaFile.readLong();
		long skiplistSize = metaFile.readLong();
		SkipList = new ArrayList<TDFSIntervaIndex.TSkipListElem>();
		for (int elemIndex = 0; elemIndex < skiplistSize; ++elemIndex) {
			SkipList.add(new TSkipListElem(metaFile.readLong(), metaFile.readDouble()));

		}
		metaFile.close();

		
	}
	
	
	public TDFSIntervaIndex(String sourceFilePath, String indexFilePath, FileSystem hdfs, Configuration config) 
																			throws IOException {	
		
		boolean preSort = false;
		boolean calcOptimalCheckpointInterval = false;
		boolean buildIndex = true;
		String sortedFile = sourceFilePath + ".sorted";
		
		if (preSort) {//sort intervals	
			hdfs.delete(new Path(sortedFile), true);	
			JobConf jobConfig = new JobConf(config);
			jobConfig.setJobName("sort_intervals");
			
			jobConfig.setMapOutputKeyClass(dfs_interval_index.TInterval.class);
			jobConfig.setMapOutputValueClass(NullWritable.class);			
			
		    jobConfig.setOutputKeyClass(dfs_interval_index.TInterval.class);
			jobConfig.setOutputValueClass(NullWritable.class);
			

			jobConfig.setMapperClass(dfs_interval_index.TSortMR.Map.class);
			jobConfig.setReducerClass(dfs_interval_index.TSortMR.Reduce.class);
			jobConfig.setInputFormat(TextInputFormat.class);
			jobConfig.setOutputFormat(SequenceFileOutputFormat.class);		
			jobConfig.setNumReduceTasks(1);
			
			jobConfig.setJar("/home/arslan/src/1d_interval_index/src/hadoop/dfs_interval_index.jar");
			
			FileInputFormat.setInputPaths(jobConfig, new Path(sourceFilePath));
			FileOutputFormat.setOutputPath(jobConfig, new Path(sortedFile));
			try {
				JobClient.runJob(jobConfig);
			} catch ( java.lang.IllegalArgumentException exception) {
				System.out.println("stupid exception");
			}
		}
		
		Path sortedFilePath = new Path(sortedFile + "/part-00000");
		
		if (calcOptimalCheckpointInterval) {//
		    Path overlappingsFilePath = new Path(sourceFilePath + ".overallpings");
		    hdfs.delete(overlappingsFilePath, true);	
		    FSDataOutputStream overlappingsFile = hdfs.create(overlappingsFilePath);		    
		    ArrayList<Integer> overlappingsSample = new ArrayList<Integer>();		    
		    SequenceFile.Reader reader = new SequenceFile.Reader(hdfs, sortedFilePath, config);
			PriorityQueue<Double> rightBordersHeap = new PriorityQueue<Double>();
			TInterval key = new TInterval();
			NullWritable nullWritable = NullWritable.get();	
			int processed = 0;
			while (reader.next(key, nullWritable)) {
				while (rightBordersHeap.peek() != null) {
					if (rightBordersHeap.peek() < key.Start) {
						rightBordersHeap.poll();
					} else {
						break;
					}
				}
				overlappingsSample.add(rightBordersHeap.size());
				rightBordersHeap.add(key.End);
				{
					++processed;
					
					if (processed % 500000 == 0) {
						System.out.println(String.format("..processed, %d", processed));
					}
					if (processed >= 1000000) {
						break;
					}
				}
			}
			overlappingsFile.close();
			{
				double avgOverlapping = 0.0;
				for (int pos = 0; pos < overlappingsSample.size(); ++pos) {
					avgOverlapping += overlappingsSample.get(pos);
				}
				avgOverlapping = avgOverlapping / overlappingsSample.size();
				System.out.println(String.format("Avg overlapping: %f", avgOverlapping));
				
				double maxMemoryUsage = overlappingsSample.size();//spaceFactor == 1
				for (CheckpointInterval = 1; CheckpointInterval <= overlappingsSample.size(); ++CheckpointInterval) {
					int offset = 0;
					Long memoryUsage = new Long(0);
					while (offset < overlappingsSample.size()) {
						memoryUsage += overlappingsSample.get(offset);
						offset += CheckpointInterval;
					}
					if (memoryUsage <= maxMemoryUsage) {
						break;
					}
				}
			}
			
		}
		
		
		IndexFilePath = indexFilePath;
		FSDataOutputStream indexFile = hdfs.create(new Path(IndexFilePath + ".index"));
		FSDataOutputStream checkpointsFile = hdfs.create(new Path(IndexFilePath + ".checkpoints"));
		FSDataOutputStream metaFile = hdfs.create(new Path(IndexFilePath + ".meta"));
		metaFile.writeLong(CheckpointInterval);
		
		System.out.println(String.format("Optimal checkpoint interval: %d", CheckpointInterval));
				
		if (buildIndex) {
			SkipList = new ArrayList<TSkipListElem>();

			PriorityQueue<TRightBorderSorter> rightBordersHeap = new PriorityQueue<TRightBorderSorter>();
			SequenceFile.Reader reader = new SequenceFile.Reader(hdfs, sortedFilePath, config);
			long pageIndex = 0;
			int recordIndex = 0;
			TInterval previousInterval = null;
			TInterval key = new TInterval();
			NullWritable nullWritable = NullWritable.get();	
			while (reader.next(key, nullWritable)) {
				if (recordIndex % 100000 == 0) {
					System.out.println(String.format("..processed %d", recordIndex));
				}
				if (recordIndex % CheckpointInterval == 0) {
					{//leave only intervals which continue
						
						while (rightBordersHeap.peek() != null) {
							if (rightBordersHeap.peek().Interval.End < key.Start) {
								rightBordersHeap.poll();
							} else {
								break;
							}
						}
					}
					try {
						indexFile.writeLong(checkpointsFile.size());
						indexFile.writeLong(checkpointsFile.size());
						indexFile.writeLong(checkpointsFile.size());
						indexFile.writeLong(checkpointsFile.size());
						++recordIndex;
						{
							checkpointsFile.writeInt(rightBordersHeap.size());
							PriorityQueue<TRightBorderReversedSorter> rightBordersReversedHeap = 
										new PriorityQueue<TRightBorderReversedSorter>();
							Iterator<TRightBorderSorter> iterator = rightBordersHeap.iterator();
							while (iterator.hasNext()) {
								TRightBorderReversedSorter element = new TRightBorderReversedSorter(iterator.next().Interval);
								rightBordersReversedHeap.add(element);
							}
							while (rightBordersReversedHeap.peek() != null) {
								TInterval interval = rightBordersReversedHeap.poll().Interval;
								checkpointsFile.writeDouble(interval.Start);
								checkpointsFile.writeDouble(interval.End);
								checkpointsFile.writeLong(interval.Id);								
							}
							while (checkpointsFile.size() % MIN_BLOCK_SIZE != 0) {
								checkpointsFile.writeInt(0);
							}
						}
					} catch (IOException exception) {
						System.err.println(exception);
					}
				}
				rightBordersHeap.add(new TRightBorderSorter(key));
				long prevPositionInIndex = indexFile.size();
				indexFile.writeDouble(key.Start);
				indexFile.writeDouble(key.End);
				indexFile.writeLong(key.Id);
				indexFile.writeLong(key.Id);
				++recordIndex;
				//System.out.println(String.format("%f - %f - %d", key.Start, key.End, key.Id));
				
				long newPageIndex = prevPositionInIndex / PAGE_SIZE;
				if (SkipList.size() == 0 || (newPageIndex > pageIndex) && (previousInterval != null) &&
						key.Start > previousInterval.Start) {
					SkipList.add(new TSkipListElem(prevPositionInIndex, key.Start));
					pageIndex = newPageIndex;
				}
				previousInterval = new TInterval(key);
			}
			SkipList.add(new TSkipListElem(indexFile.size(), previousInterval.Start + 1));
			
			
			{
				metaFile.writeLong(SkipList.size());
				for (int index = 0; index < SkipList.size(); ++index) {
					metaFile.writeLong(SkipList.get(index).Offset);
					metaFile.writeDouble(SkipList.get(index).MinLeftBorder);
				}
			}
			
			indexFile.close();
			checkpointsFile.close();
			metaFile.close();
			IndexFile = hdfs.open(new Path(IndexFilePath + ".index"));
			CheckpointsFile = hdfs.open(new Path(IndexFilePath + ".checkpoints"));			
		}
	}

	private void SearchInCheckpoint(ArrayList<TInterval> results, 
									final long checkpointLocation, 
									final double start, 
									final double end) throws IOException {
		CheckpointsFile.seek(checkpointLocation);
		int recordsCount = CheckpointsFile.readInt();
		while (recordsCount > 0) {
			double intervalStart = CheckpointsFile.readDouble();
			double intervalEnd = CheckpointsFile.readDouble();
			long intervalId = CheckpointsFile.readLong();
			--recordsCount;
			if (intervalEnd < start) {
				break;
			}
			TInterval result = new TInterval(intervalStart, intervalEnd, intervalId);
			results.add(result);
		}
	}
	
	public int Search(final double start, final double end) throws IOException {
		System.out.println(String.format("search %f-%f", start, end));
		if (end <= start) {
			return 0;
		}		
		if (SkipList.size() == 0) {
			return 0;
		}
		if (SkipList.get(0).MinLeftBorder > end) {
			return 0;
		}
		if (SkipList.get(SkipList.size() - 1).MinLeftBorder <= start) {
			return 0;
		}
		
		long uploadRightBorder = 0;
		int lastRecordIndex = SkipList.size() - 1;
		//TODO: rewrite to binary search
		for (int recordIndex = 0; recordIndex < SkipList.size(); ++recordIndex) {
			double chunkStartValue = SkipList.get(recordIndex).MinLeftBorder;
			long chunkOffset = SkipList.get(recordIndex).Offset;
			if (chunkStartValue > end || recordIndex == lastRecordIndex) {
				uploadRightBorder = chunkOffset;
				//System.out.println(String.format("first chunk: %f-%d", chunkStartValue, recordIndex));
				break;
			}
		}
		
		ArrayList<TInterval> results = new ArrayList<TInterval>();
	
		byte[] buffer = new byte[PAGE_SIZE];
		double nextIntervalStart = end;
		while (uploadRightBorder > 0) {
			final int RECORD_SIZE = 32;
			final int FIELD_SIZE = 8;
			long uploadFrom = (long)(Math.max(0, Math.ceil((double)(uploadRightBorder - PAGE_SIZE) / RECORD_SIZE))) * RECORD_SIZE;
			
			
			int length = (int)(uploadRightBorder - uploadFrom);
			//System.out.println(String.format("upload: %d-%d", uploadFrom, uploadFrom + length));
			
			IndexFile.readFully(uploadFrom, buffer, 0, PAGE_SIZE);
			ByteBuffer bufferAsStream = ByteBuffer.wrap(buffer);
			int localProfit = 0;
			boolean lastChunk = false;
			for (int recordIndex = (length / RECORD_SIZE) - 1; recordIndex > -1; --recordIndex) {
				bufferAsStream.position(recordIndex * RECORD_SIZE);
				double intervalStart = bufferAsStream.getDouble();
				double intervalEnd = bufferAsStream.getDouble();
				//System.out.println(String.format(".. interval: %f - %f", intervalStart, intervalEnd));
				long intervalId = bufferAsStream.getLong();
				long globalRecordIndex = (uploadFrom / RECORD_SIZE) + recordIndex;
				boolean isLinkToCheckPoints = globalRecordIndex % CheckpointInterval == 0;
				if (isLinkToCheckPoints) {
					long checkpointPosition = bufferAsStream.getLong();
					//checkpoints never appear between intervals having same start value, so (less or equal) is allowed
					if (nextIntervalStart <= start) {
						SearchInCheckpoint(results, checkpointPosition, start, end);						
						lastChunk = true;
						break; // full stop 
					} else {
						continue;
					}
				}
				
				if (intervalEnd >= start && intervalStart <= end) {
					//add to results
					TInterval result = new TInterval();
					result.Id = intervalId;
					result.Start = intervalStart;
					result.End = intervalEnd;
					results.add(result);
					localProfit += 1;
				}
				nextIntervalStart = intervalStart;
			}
			//System.out.println(String.format("local profit: %s", localProfit));
			if (lastChunk) {
				break;
			}
			uploadRightBorder = uploadFrom;
		}
		return results.size();
	}	
	
	

	
}
