import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



/**
 * Class POPAlgorithm
 * 	Prototype algorithm which runs as a series of Map-reduce jobs 
 * for executing in distributed system
 * 
 * @author Deepthi Sistla
 */
public class POPAlgorithm extends Configured implements Tool {
	
	static int TOTAL_ATTRIBUTES = 24;

	/**
	 * 
	 * Attribute Mapper which gets executed for each continuous attribute
	 * in the given dataset
	 *      
	 */
	public static class AttributeMapper extends Mapper<Object,Text,Text,Text>{
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
				
				int ATTRIBUTE_NUM = Integer.parseInt(context.getConfiguration().get("attribute_num"));
					
					String[] valueString = value.toString().split(",");
					if(valueString.length != TOTAL_ATTRIBUTES + 1)
						throw new ArrayIndexOutOfBoundsException("Mapper " + ATTRIBUTE_NUM + ": Invalid input line format encountered");
					
					String outputKey = valueString[ATTRIBUTE_NUM];
					
					StringBuffer outputValueBuffer = new StringBuffer();
					for(int i= 0; i< valueString.length; i++)
					{
						if (i != ATTRIBUTE_NUM)
							outputValueBuffer.append(valueString[i]).append(",");
					}
					String outputValue = outputValueBuffer.substring(0, outputValueBuffer.length() - 1);
					context.write(new Text(outputKey), new Text(outputValue));
				}
		
	}



	/**
	 *  Reducer (for each continuous attribute) which is executed for each key 
	 *  output from mapper.
	 *         
	 */
	public static class AttributeReducer extends Reducer<Text, Text, Text, Text>{
		private String lastClassLabel = "";  
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			int ATTRIBUTE_NUM = Integer.parseInt(context.getConfiguration().get("attribute_num"));
			
			try 
			{
				Map<String, List<String>> finalValuesFirstPart = new HashMap<String, List<String>>();
				Map<String, List<String>> finalValuesRemaining = new HashMap<String, List<String>>();
			
			    for (Text valueText: values) {
			        String[] inputValueArray = valueText.toString().split(",");
			        if (inputValueArray.length != TOTAL_ATTRIBUTES)
			        	throw new ArrayIndexOutOfBoundsException("Reducer " + ATTRIBUTE_NUM + ": Invalid input line format encountered");
			        
			        String currClassLabel = inputValueArray[inputValueArray.length - 1];
		        	
		        	StringBuffer strBuffer = new StringBuffer(); 
		        	for(int i = 0; i < inputValueArray.length; i++)
		        	{
		        		if(i == ATTRIBUTE_NUM)
		        		{
		        			strBuffer.append(key.toString()).append(",");
		        		}
		        		strBuffer.append(inputValueArray[i]).append(",");
		        	}
		        	
		        	String actualString = strBuffer.substring(0, strBuffer.length() - 1);
		        	
			        if(lastClassLabel.equals(currClassLabel))
				    {
			        	List<String> currList;
			        	if (finalValuesFirstPart.get(currClassLabel) == null)
			        		currList = new ArrayList<String>();
			        	else
			        		currList = (ArrayList<String>) finalValuesFirstPart.get(currClassLabel);
			        	
			        	System.out.println("Curr List size - " + currList.size());
			        	System.out.println("Adding - " + actualString);
			        	currList.add(actualString);
			        	
			        	System.out.println("Adding Curr list to finalValuesFirstPart");
			        	finalValuesFirstPart.put(currClassLabel, currList);
			        }
			        else
			        {
			        	List<String> currList;
			        	if (finalValuesRemaining.get(currClassLabel) == null)
			        		currList = new ArrayList<String>();
			        	else
			        		currList = (ArrayList<String>) finalValuesRemaining.get(currClassLabel);
			        	
			        	currList.add(actualString);
			        	finalValuesRemaining.put(currClassLabel, currList);
			        }
			    }
			    
			    Map<String, List<String>> sortedfinalValuesRemaining = new TreeMap<String, List<String>>(finalValuesRemaining);
			    List<String> finalValues = new ArrayList<String>();
			    
			    
			    for(Map.Entry<String, List<String>> entry : finalValuesFirstPart.entrySet())
			    	finalValues.addAll(entry.getValue());
			    for(Map.Entry<String, List<String>> entry : sortedfinalValuesRemaining.entrySet())
			    	finalValues.addAll(entry.getValue());
			    			    
			    Map<String, String> finalMap = new HashMap<String, String>();
			    
			    String lastKey = "";
			    for(String finalValue : finalValues)
			    { 
			    	String currClassLabel = finalValue.split(",")[TOTAL_ATTRIBUTES];
			    	String weakness = "0";
			    	if (currClassLabel.equals(lastClassLabel))
			    		weakness = "1";
			    	else if (lastKey != "" && finalMap.get(lastKey).toString() == "1")
			    		finalMap.put(lastKey, "0");
			    		
			    	
			    	lastKey = finalValue;
			    	finalMap.put(finalValue, weakness);
			    	lastClassLabel = currClassLabel;
			    }
			    if (lastKey != "")
		    		finalMap.put(lastKey, "0");
			    
			    for(Map.Entry<String, String> entry : finalMap.entrySet())
			    	context.write(new Text(entry.getKey()), new Text(entry.getValue()));
			    	
			    
		      }
		      catch(Exception ex) {
		    	  System.out.println(ex.getMessage());
		    	  ex.printStackTrace();
		      }
		
		}
	}
	/**
	 * Weakness mapper
	 * 
	 * @author Deepthi Sistla
	 *
	 */
	
	public static class WeaknessMapper extends Mapper<Object, Text, Text, Text> {

		
		public void map(Object key, Text value, Context context)
		throws IOException, InterruptedException {
			
			String[] valueString = value.toString().split("\\t");
			if(valueString.length != 2)
				throw new ArrayIndexOutOfBoundsException("Weakness Mapper: Invalid input line format encountered");
			
			String outputKey = valueString[0];
			String outputValue = valueString[1];
			
			context.write(new Text(outputKey), new Text(outputValue));
		}
	}
	
	/**
	 * Weakness Reducer
	 * 
	 * @Author Deepthi Sistla   
	 */

	public static class WeaknessReducer extends Reducer<Text, Text, Text, Text>{
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
		
			int totalWeakness = 0;
		    for (Text valueText: values) {
		        totalWeakness += Integer.parseInt(valueText.toString());
		    }
		    context.write(new Text(key.toString()), new Text(totalWeakness + ""));
		}
	}

	/**
	 * Mapper for discrete attributes
	 * 
	 * @author Deepthi Sistla
	 *
	 */
	public static class DiscreteAttributeMapper extends Mapper<Object, Text, Text, Text> {

		
		public void map(Object key, Text value, Context context)
		throws IOException, InterruptedException {
			
			int ATTRIBUTE_NUM = Integer.parseInt(context.getConfiguration().get("attribute_num"));
			
			String[] valueString = value.toString().split("\\s+");
			String[] attributeArray = valueString[0].trim().split(",");
			if(attributeArray.length != TOTAL_ATTRIBUTES + 1)
				throw new ArrayIndexOutOfBoundsException("Mapper " + ATTRIBUTE_NUM + ": Invalid input line format encountered");
			
			String outputKey = attributeArray[ATTRIBUTE_NUM];
			
			StringBuffer outputValueBuffer = new StringBuffer();
			for(int i= 0; i< attributeArray.length; i++)
			{
				if (i != ATTRIBUTE_NUM)
					outputValueBuffer.append(attributeArray[i]).append(",");
			}
			outputValueBuffer.append(valueString[1].trim());
			String outputValue = outputValueBuffer.toString();
			context.write(new Text(outputKey), new Text(outputValue));
		}
	}
	
	/**
	 * Reducer for discrete attributes
	 * 
	 * @author Deepthi Sistla
	 *
	 */
	public static class DiscreteAttributeReducer extends Reducer<Text, Text, Text, Text>{
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			int ATTRIBUTE_NUM = Integer.parseInt(context.getConfiguration().get("attribute_num"));
		
			int minWeakness = 100;
			List<String> valueList = new ArrayList<String>();
		    for (Text valueText: values) {
		    	valueList.add(valueText.toString());
		    	
		    	String[] valueArray = valueText.toString().split(",");
		    	int currWeakness = Integer.parseInt(valueArray[valueArray.length - 1]);
		    	if(currWeakness < minWeakness)
		    		minWeakness = currWeakness;
		    }
		    
		    for (String valueText : valueList) {
		    	String[] valueArray = valueText.split(",");
		    	int currWeakness = Integer.parseInt(valueArray[valueArray.length - 1]);
		    	if(currWeakness != minWeakness)
		    		currWeakness++;
		    	
		    	if(currWeakness != TOTAL_ATTRIBUTES) {
		    		valueArray[valueArray.length - 1] = currWeakness + "";
			    	
		    		StringBuffer strBuffer = new StringBuffer(); 
		        	for(int i = 0; i < valueArray.length; i++)
		        	{
		        		if(i == ATTRIBUTE_NUM)
		        		{ 
		        			strBuffer.append(key.toString()).append(",");
		        		}
		        		strBuffer.append(valueArray[i]).append(",");
		        	}
		        	
		        	String actualString = strBuffer.substring(0, strBuffer.length() - 3);
			    	context.write(new Text(actualString), new Text(currWeakness + ""));
		    	}
		    }
		}
	}

	/**
	 * Driver function which triggers the map-reduce jobs
	 * for all continuous and discrete attributes.
	 * 
	 */
	public int run(String[] args) throws Exception {
		
		for(int i=0; i<TOTAL_ATTRIBUTES;i++){
			if(i == 1 || i >= 14)
				AttributeJob(args[0],"/POPAlgorithmOutput/Attribute-" + i,i);			
     	}
	
		String destFile = "/POPAlgorithmOutput/ReducersOutput/ReducersOutputfile";
		String finalOutputPath = "/POPAlgorithmOutput/FinalOutput";
		
		Configuration conf = new Configuration();
		try
		{
			
			FileSystem hdfs = FileSystem.get(conf);
						
			for(int i=0;i<TOTAL_ATTRIBUTES - 1;i++){
				int next_attribute = i+1;
				if(i == 1)
					next_attribute = 14;
				if(i == 1 || i >= 14)
					FileUtil.copyMerge(hdfs, new Path("/POPAlgorithmOutput/Attribute-"+ i), hdfs, new Path("/POPAlgorithmOutput/Attribute-" + next_attribute + "/OutputTillPreviousReducer"), false, conf, null);
			}
			FileUtil.copyMerge(hdfs, new Path("/POPAlgorithmOutput/Attribute-"+ (TOTAL_ATTRIBUTES - 1)), hdfs, new Path(destFile), false, conf, null);
		}
		catch(IOException ex)
		{}
		weaknessJob(destFile, finalOutputPath);
		
		
		discreteAttributeJob(finalOutputPath + "/part-r-00000","/POPAlgorithmOutput/Attribute-0",0);
		for(int i=2; i<14;i++){
			int prevAttribute = i - 1;
			if(i == 2)
				prevAttribute = 0;
			discreteAttributeJob("/POPAlgorithmOutput/Attribute-" + prevAttribute + "/part-r-00000"
								,"/POPAlgorithmOutput/Attribute-"+i,i);
     	}
		
		return 0;

	}
	
	/**
	 * Set job configuration for Continuous Attribute Mapper
	 * @param inputPath		DFS path where the input dataset file is present
	 * @param outputPath	DFS path where the final output is to be stored
	 * @param attNum		Current attribute number
	 * @throws Exception
	 */
	private void AttributeJob(String inputPath, String outputPath,int attNum)
			throws Exception {

		Job job = new Job(new Configuration(), attNum+"-Attribute-Job");
		job.getConfiguration().set("attribute_num", attNum + "");

		job.setJarByClass(POPAlgorithm.class);

		//set the mapper class
		job.setMapperClass(AttributeMapper.class);

		//set the reducer class
		job.setReducerClass(AttributeReducer.class);
		
		job.setNumReduceTasks(1);
	
		// set the type of the output key and value for the Map & Reduce
		// functions
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(inputPath)); // setting the input files for the job
		FileOutputFormat.setOutputPath(job, new Path(outputPath)); // setting the output files for the job
		job.waitForCompletion(true); // wait for the job to complete
		
	}

	/**
	 * Set job configuration for Discrete Attribute Mapper
	 * @param inputPath		DFS path where the input dataset file is present
	 * @param outputPath	DFS path where the final output is to be stored
	 * @param attNum		Current attribute number
	 * @throws Exception
	 */
	private void discreteAttributeJob(String inputPath, String outputPath,int attNum)
			throws Exception {

		Job job = new Job(new Configuration(), attNum+"-Attribute-Job");
		job.getConfiguration().set("attribute_num", attNum + "");

		job.setJarByClass(POPAlgorithm.class);

		//set the mapper class
		job.setMapperClass(DiscreteAttributeMapper.class);

		//set the reducer class
		job.setReducerClass(DiscreteAttributeReducer.class);
		
		job.setNumReduceTasks(1);
	
		// set the type of the output key and value for the Map & Reduce
		// functions
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(inputPath)); // setting the input files for the job
		FileOutputFormat.setOutputPath(job, new Path(outputPath)); // setting the output files for the job
		job.waitForCompletion(true); // wait for the job to complete
		
	}
	
	/**
	 * Function which launches the weakness map and reduce jobs
	 * for summarizing the total weakness from continuous attributes 
	 * @param inputPath
	 * @param outputPath
	 * @throws Exception
	 */
	private void weaknessJob(String inputPath, String outputPath)
			throws Exception {
		Job job = new Job(new Configuration(), "WeaknessJob");

		job.setJarByClass(POPAlgorithm.class);

		//set the mapper class
		job.setMapperClass(WeaknessMapper.class);

		
		//set the reducer class
		job.setReducerClass(WeaknessReducer.class);
		
		job.setNumReduceTasks(1);
	
		// set the type of the output key and value for the Map & Reduce
		// functions
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(inputPath)); // setting the input files for the job
		FileOutputFormat.setOutputPath(job, new Path(outputPath)); // setting the output files for the job
			
		job.waitForCompletion(true); // wait for the job to complete
		
	}
	
	/**
	 * Main function which triggers the POP Algorithm execution
	 * @param args	args[0] is the DFS path where input file is present 
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new POPAlgorithm(), args);
		System.exit(res);
	}

}