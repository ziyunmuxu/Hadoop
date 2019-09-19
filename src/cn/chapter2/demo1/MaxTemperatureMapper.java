package cn.chapter2.demo1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


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
 *问题1：hadoop2中的Mapper是一个类，而不是接口
 *问题2：quality在正则表达式[01459]这个范围才表示有效数据
 *
 */


public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	private static final int MISSING = 9999;
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String year = line.substring(15,19);
		int airTemperature;
		
		if(line.charAt(87) == '+'){//parseInt 不能前面加上+号
			airTemperature = Integer.parseInt(line.substring(88, 92));
		}else{
			airTemperature = Integer.parseInt(line.substring(87, 92));
		}
		
		String quality = line.substring(92, 93);
		if(airTemperature != MISSING && quality.matches("[01459]")){
			context.write(new Text(year), new IntWritable(airTemperature));
		}
	}

	
	
	

}
