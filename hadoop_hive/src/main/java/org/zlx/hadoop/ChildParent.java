/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.StringTokenizer;

@Slf4j
public class ChildParent {
    static String childSplit="child$@";

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text>{

        private final static IntWritable one = new IntWritable(1);



        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            log.info("mapxxxx, key:{},value:{}",key.toString(),value.toString());

            String child =itr.nextToken();
            String parent =itr.nextToken();
            //左表,  按照key来进行关联
            context.write(new Text(parent), new Text(childSplit+child));

            //右表,
            context.write(new Text(child), new Text(parent));
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,Text,Text,Text> {


        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            log.info("reducexxx, key:{},value:{}",key.toString(),values.toString());

            List <String > childList=new ArrayList<>();
            List <String > grandFatherList=new ArrayList<>();
            values.forEach(
                    value->{
                        if(value.toString().contains(childSplit)){
                            childList.add(value.toString());
                        }
                        else{
                            grandFatherList.add(value.toString());
                        }
                        log.info("reducexxx, key:{},value:{}",key.toString(),value.toString());
                    }
            );

            childList.forEach(
                    child->{
                        grandFatherList.forEach(
                                grandFather-> {
                                    try {
                                        context.write(new Text(child), new Text(grandFather));
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    }
                                }
                        );
                    }
            );

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        out("xx");
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(ChildParent.class);

        job.setMapperClass(TokenizerMapper.class);

//        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }


        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]+"_"+Calendar.getInstance().get(Calendar.HOUR_OF_DAY)+Calendar.getInstance().get(Calendar.MINUTE)));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void out(Object o){
        System.out.println("xxxx"+o);
    }
}
