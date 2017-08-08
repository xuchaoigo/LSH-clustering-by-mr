package com.baidu.LSHClustering;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.util.Bytes;
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

public class LSHGenFile extends Configured implements Tool {   
	static final Logger logger = LoggerFactory.getLogger(LSHGenFile.class);	
	public static class MapReducePathFilter implements PathFilter {
		public boolean accept(Path path) {
			return !path.getName().endsWith("._COPYING_");
		}
	}
	
	protected static class MapMinHash extends Mapper<LongWritable, Text, Text, Text>{		
		private final static int AND_NUM = 4;
		private final static int OR_NUM = 4;
		private final static String AND_STR = "_";
		private final static String OR_STR = "|";
		private final static String SPACE_STR = " ";
		private final static String SAMPLE_STR = "_";
		private static HashFunc [][] HashFuncArray = new HashFunc[OR_NUM][AND_NUM];	
		protected Text outKey = new Text();
		protected Text outVal = new Text();
		@Override
		public void setup(Context context) throws IOException,InterruptedException {	
			
		}
		@Override
		public void cleanup(Context context) throws IOException,InterruptedException {
			
		}		
		
		static{				
			HashFuncArray[0][0] = new HashFunc(32364149,64476557,Integer.MAX_VALUE);
			HashFuncArray[0][1] = new HashFunc(32390651,91927692,Integer.MAX_VALUE);
			HashFuncArray[0][2] = new HashFunc(32435737,27687085,Integer.MAX_VALUE);
			HashFuncArray[0][3] = new HashFunc(32460203,58911028,Integer.MAX_VALUE);			
			
			HashFuncArray[1][0] = new HashFunc(52505181,668776768,Integer.MAX_VALUE);
			HashFuncArray[1][1] = new HashFunc(52505675,196527693,Integer.MAX_VALUE);
			HashFuncArray[1][2] = new HashFunc(52523057,727687087,Integer.MAX_VALUE);
			HashFuncArray[1][3] = new HashFunc(52537663,258911065,Integer.MAX_VALUE);
			
			HashFuncArray[2][0] = new HashFunc(72550203,7763768,Integer.MAX_VALUE);
			HashFuncArray[2][1] = new HashFunc(72568921,1965527,Integer.MAX_VALUE);
			HashFuncArray[2][2] = new HashFunc(72610547,6727687,Integer.MAX_VALUE);
			HashFuncArray[2][3] = new HashFunc(72622087,3258911,Integer.MAX_VALUE);
			
			HashFuncArray[3][0] = new HashFunc(92668303,76763723,Integer.MAX_VALUE);
			HashFuncArray[3][1] = new HashFunc(92678199,13965522,Integer.MAX_VALUE);
			HashFuncArray[3][2] = new HashFunc(92732349,62727685,Integer.MAX_VALUE);
			HashFuncArray[3][3] = new HashFunc(92737015,32958911,Integer.MAX_VALUE);	
		}		

		protected static class HashFunc{
			int coef,bias,mod;
			public HashFunc(int c, int b, int m){
				coef = c;
				bias = b;
				mod = m;
			}
			public Long hash(Long key){
				return  (coef * key + bias) % mod;
			}
		}
						
		@Override
		protected void map(LongWritable key, Text val, Context context)
				throws IOException, InterruptedException{			
			String [] SampleArray = val.toString().split(" ");
			if(SampleArray.length <= 2){
				logger.error("invalid line: line={}", val);
				context.getCounter(WHITELIST_COUNTER, "Parse Line Failed").increment(1);
				return;
			}			
			context.getCounter("GenFile", "raw sample num").increment(1);
			for(int space = 0; space < OR_NUM; space++){						
				long [] MinHash = new long[AND_NUM];				
				for(int i = 0; i < AND_NUM; i++){
					long MinHashVal = Long.MAX_VALUE;
					long MinHashIdx = 0;
					for(int j = 2; j < SampleArray.length; j++){
						long Key = Long.parseLong(SampleArray[j], 16);
						long HashVal = HashFuncArray[space][i].hash(Key);
						if(HashVal < MinHashVal){
							MinHashVal = HashVal;
							MinHashIdx = Key;
						}
					}			
					MinHash[i] = MinHashIdx;
				}		
				
				StringBuilder SbOneSpace = new StringBuilder();
				for(int i = 0; i < AND_NUM; i++){
					SbOneSpace.append(String.valueOf(MinHash[i]));
					SbOneSpace.append(AND_STR);
				}
				SbOneSpace.deleteCharAt(SbOneSpace.length()-1);

				outKey.set(Bytes.toBytes(SbOneSpace.toString()));
				outVal.set(Bytes.toBytes(val.toString()));
				context.write(outKey, outVal);				
			}			
		}			
	}
	
	protected static class RedMinHash extends Reducer<Text, Text, Text, Text>{
		protected Text outKey = new Text();
		protected Text outVal = new Text();
		private final static String SAMPLE_STR = "_";
		
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {	
		if(key.toString().isEmpty()){
			logger.error("bucket name == empty!");
			return;
		}

		StringBuilder SbSampleList = new StringBuilder();
		for(Text val: values){			
			String sample = new String(val.toString());
			SbSampleList.append(sample);
			SbSampleList.append(SAMPLE_STR);
			context.getCounter("GenFile", "sample in bucket num").increment(1);
		}
		SbSampleList.deleteCharAt(SbSampleList.length()-1);	
			
		outVal.set(SbSampleList.toString());
		context.write(outKey, outVal);
		context.getCounter("GenFile", "bucket num").increment(1);
		return;
	}		
	
	public void setup(Context context) throws IOException,InterruptedException {		
	}
	@Override
	public void cleanup(Context context) throws IOException,InterruptedException {		
	}	
	}
	    
	public int waitJob(Job job) throws ClassNotFoundException, IOException,
			InterruptedException {
		int jobResult = job.waitForCompletion(true) ? 0 : 1;
		logger.info("jobResult={}", jobResult);
		return jobResult;
	}
	
	public int run(String[] args) throws Exception {
		Path input = new Path(args[0]);
		logger.info("input={}", input);
		
		Path output = new Path(args[1]);
		logger.info("output={}", output);
			
		Configuration conf = getConf();

		logger.info("LSHGenFile v2");
		Job job = new Job(conf, "LSH Clustering");
		job.setJarByClass(this.getClass());		
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job, input);
		TextInputFormat.setInputPathFilter(job, MapReducePathFilter.class);
		TextInputFormat.setMinInputSplitSize(job, 1024*1024*1024);		
		
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, output);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
       	
		job.setMapperClass(MapMinHash.class);
		job.setReducerClass(RedMinHash.class);
		
		job.setNumReduceTasks(30);		
		return job.waitForCompletion(true)? 0: 1;
	}

	public static void main(String[] args) throws Exception{
		System.exit(ToolRunner.run(new LSHGenFile(), args));
	}
}
