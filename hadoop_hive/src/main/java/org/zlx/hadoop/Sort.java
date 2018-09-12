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
import java.util.Calendar;

@Slf4j
public class Sort {

    private final static IntWritable one = new IntWritable(1);

    public static class TokenizerMapper
            extends Mapper<Object, Text, IntWritable, IntWritable>{



        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            log.info("mapxxxx, key:{},value:{}",key.toString(),value.toString());
            int v=Integer.valueOf(value.toString());

            context.write(new IntWritable(v), one);
        }
    }

    public static class IntSumReducer
            extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {


        int order=1;

        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            values.forEach(
                   v-> log.info("reducexxx, key:{},value:{}",key.toString(),v.toString())
            );
            context.write(new IntWritable(order), key);
            order++;
        }
    }

    public static class Combiner
            extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            values.forEach(
                    v-> log.info("combinexxxx, key:{},value:{}",key.toString(),v.toString())
            );
            context.write(key, one);
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
        Job job = Job.getInstance(conf, "sort");
        job.setJarByClass(Sort.class);

        job.setMapperClass(TokenizerMapper.class);
//不能设置 combiner, 因为reduce的输出是把 相关<order,key> . 也就是把mapper的key做 reduce的value输出
// 如果两次调用reduce,那么最后reduce的输入key 就变原来的order，因此需要单独写 combiner
        job.setCombinerClass(Combiner.class);

        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }


        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]+""+Calendar.getInstance().get(Calendar.HOUR_OF_DAY)+Calendar.getInstance().get(Calendar.MINUTE)));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void out(Object o){
        System.out.println("xxxx"+o);
    }
}
