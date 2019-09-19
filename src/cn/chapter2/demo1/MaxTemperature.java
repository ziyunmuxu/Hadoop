package cn.chapter2.demo1;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 参考：
 * https://blog.csdn.net/weixin_44148703/article/details/89177683
 * https://blog.csdn.net/chenxun_2010/article/details/78242096
 * @author ziyunmuxu
 *
 *版本：
 *hadoop：hadoop-2.8.5
 *hadoop权威指南第4版
 *
 *该类负责运行MapReduce作业
 *
 */

public class MaxTemperature {
	public static void main(String[] args) throws Exception {
		if(args.length != 2){
			System.err.println("Usage: MaxTemperature <input path> <output path>");
			System.exit(-1);
		}
		
		Job job = new Job();
		job.setJarByClass(MaxTemperature.class);
		job.setJobName("Max Temperature");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(MaxTemperatureMapper.class);
		job.setReducerClass(MaxTemperatureReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true)? 0:1);
		
	}
}
