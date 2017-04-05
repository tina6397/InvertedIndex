import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.*;
import java.util.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//debugg :1 and weird entry for keys
public class WordCount9 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text word = new Text();





    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

                      String string_value = value.toString();
                      String[] array_value = string_value.split("\t");
                      StringTokenizer itr = new StringTokenizer(array_value[1]);
                        String id = array_value[0];
                        // String space = itr.nextToken();
                        // System.out.println("MAPPER ID"+id);


      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, new Text(id));
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      // int sum = 0;
      HashMap<String,Integer> hmap = new HashMap<String,Integer>();

      for (Text val : values) {
        String key_value=val.toString();
        if(key_value!=""){
          if(hmap.containsKey(key_value)){
            //update value
            hmap.put(key_value,hmap.get(key_value)+1);
          }else{
            //create new key
            hmap.put(key_value,1);
          }

        }
      }

      String sum = "";
      //Print results to sum
      for(String t : hmap.keySet()){
        if(t.length()>3){
          sum+= t + ":" +hmap.get(t)+"; ";

        }
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount9.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
