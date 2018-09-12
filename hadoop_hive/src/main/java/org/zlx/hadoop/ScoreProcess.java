package org.zlx.hadoop;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Calendar;
import java.util.StringTokenizer;

/**
 * Created by @author linxin on 25/05/2018.  <br>
 */
@Slf4j
public class ScoreProcess{

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private static  IntWritable value2=new IntWritable(1) ;
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {


            StringTokenizer itr = new StringTokenizer(value.toString());
            if(itr.countTokens()==2){
                word.set(itr.nextToken());
                value2.set( Integer.valueOf(itr.nextToken()));
            }
            log.info("map.out key:{},vallue:{}",word,value2);
            context.write(word,value2);


        }
    }


    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {


        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            // log.info("reduce.input key:{},value:{}",key.toString(),values.iterator().next().get());
            // 这个如果next了，那么 下面的迭代就会出问题

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: ScoreProcess <in> [<in>...] <out>");
            System.exit(2);
        }


        Job job = Job.getInstance(conf, "ScoreProcess");
        job.setJarByClass(ScoreProcess.class);

        job.setMapperClass(TokenizerMapper.class);

        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setNumReduceTasks(1);
        job.setInputFormatClass(TextInputFormat.class);
        Class<TextOutputFormat> cls = TextOutputFormat.class;
        job.setOutputFormatClass(cls);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }


        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]+""+Calendar.getInstance().get(Calendar.HOUR_OF_DAY)+Calendar.getInstance().get(Calendar.MINUTE)));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }



}
