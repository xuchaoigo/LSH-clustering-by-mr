package com.baidu.LSHClustering;
import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LSHClusterFile extends Configured implements Tool{	
	private static final Logger logger = LoggerFactory.getLogger(LSHClusterFile.class);	

	protected static class MapFind extends Mapper<LongWritable, Text, Text, Text>{
		private final static double THRESHOLD = 0.75;
		private final static String SPACE_STR = " ";
		private final static String SAMPLE_STR = "_";
		private Text outKey = new Text();
		private Text outVal = new Text();
		
		protected  static HashSet<String> StringtoSet(String Sample){
			HashSet<String> s = new HashSet<String>();
			String [] SampleArray = Sample.split(SPACE_STR);
			for(int j = 2; j < SampleArray.length; j++)
				s.add(SampleArray[j]);
			return s;
		}
		
		protected static double calJacardDis(HashSet<String> sample1,HashSet<String> sample2){		
			int intersectNum = 0;
			for(String i:sample1){
				if(sample2.contains(i))
					intersectNum += 1;
			}
			return intersectNum / (sample1.size() + sample2.size() - intersectNum + 0.0);
		}
		
		protected String cutMd5(String Sample){
			String md5 = Sample.substring(0, Sample.indexOf(SPACE_STR));	
			return md5;
		}				

		@Override
		protected void map(LongWritable key, Text val, Context context)
					throws IOException, InterruptedException{	
			String sampleStr = val.toString().trim();
			if(sampleStr.equals("")){
				return;
			}
			StringTokenizer token=new StringTokenizer(sampleStr, SAMPLE_STR);
			HashSet<String> samplesInBucket = new HashSet<String>();
			
			while(token.hasMoreElements()){ 				
				samplesInBucket.add(token.nextToken());
			}
			HashSet<String> otherSamplesInBucket = new HashSet<String>(samplesInBucket);
			HashSet<String> clusteredMd5 = new HashSet<String>();
			if(samplesInBucket.size() == 1){ 
				context.getCounter("ClusterFile", "md5 single cluster num").increment(1);				
				outKey.set(cutMd5(sampleStr));
				outVal.set(sampleStr);
				context.write(outKey,outVal);
				return;
			}	
			for(String target:samplesInBucket){
				HashSet<String> nearSample = new HashSet<String>();
				String targetMd5 = cutMd5(target);
				if(clusteredMd5.contains(targetMd5)){
					continue;
				}
				context.getCounter("ClusterFile", "md5 cluster num").increment(1);	
				clusteredMd5.add(targetMd5);
				for(String otherSample:otherSamplesInBucket){
					double distance = calJacardDis(StringtoSet(target), StringtoSet(otherSample));
					if(distance > THRESHOLD){
						context.getCounter("ClusterFile", "near num").increment(1);
						nearSample.add(otherSample);
						clusteredMd5.add(cutMd5(otherSample));
					}
					else{
						context.getCounter("ClusterFile", "far num").increment(1);
					}
				}
				otherSamplesInBucket.removeAll(nearSample);				
				
				StringBuilder SbNearSample = new StringBuilder();	
				for(String s:nearSample){
					SbNearSample.append(s);
					SbNearSample.append(SAMPLE_STR);
				}
				SbNearSample.deleteCharAt(SbNearSample.length() - 1);		
				outKey.set(targetMd5);
				outVal.set(SbNearSample.toString());
				context.write(outKey,outVal);	
			}
			return;
		}	
		
	    @Override
	    protected void setup(Context context) throws IOException,
	            InterruptedException {	

	    }
	    @Override
	    protected void cleanup(Context context) throws IOException, InterruptedException {

	    }
		
	}	
	
	protected static class RedFind extends Reducer<Text, Text, Text, Text>{
		private final static String SAMPLE_STR = "_";
		private Text outKey = new Text();
		private Text outVal = new Text();
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {		
		for(Text val: values){
			outVal.set(val.toString());
			context.write(key, outVal);
			break;
		}		
		return;
	}		
	
    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {

    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

    }

	}	

	public int run(String[] args) throws Exception {
		Path input = new Path(args[0]);
		logger.info("LSHClusterFile v4, 8hw, gj, output one file");
		logger.info("LSHClusterFile input={}", input);
		
		Path output = new Path(args[1]);
		logger.info("LSHClusterFile output={}", output);
		

		Job job = new Job(getConf(), "LSH clustering");
		job.setJarByClass(this.getClass());
		Configuration conf = job.getConfiguration();
        conf.setInt("mapreduce.job.reduce.slowstart.completedmaps", 1);
        
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job, input);
			
		job.setMapperClass(MapFind.class);
		job.setReducerClass(RedFind.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(100);
		job.setOutputFormatClass(TextOutputFormat.class);	
		TextOutputFormat.setOutputPath(job, output);
		
		return job.waitForCompletion(true)? 0: 1;
	}

	public static void main(String[] args) throws Exception{
		System.exit(ToolRunner.run(new LSHClusterFile(), args));
	}
}
