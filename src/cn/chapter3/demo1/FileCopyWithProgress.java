package cn.chapter3.demo1;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

/**
 * 3.5.3向HDFS写入数据
 * @author ziyunmuxu
 *
 */

public class FileCopyWithProgress {
	public static void main(String[] args) throws Exception {
		String localSrc = args[0];
		String dst = args[1];
		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		OutputStream out = fs.create(new Path(dst), new Progressable() {
			
			@Override
			public void progress() {
				// TODO Auto-generated method stub
				System.out.println(".");
			}
		});
		IOUtils.copyBytes(in, out, 4096, true);
	}
}
