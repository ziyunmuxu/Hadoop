package cn.chapter3.demo1;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * 3.5.2通过FileSystem API读取数据
 * @author ziyunmuxu
 *
 */
public class FileSystemCat {
	public static void main(String[] args) throws IOException {
		String uri = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		InputStream in = null;
		try{
			in = fs.open(new Path(uri));
			IOUtils.copyBytes(in, System.out, 4096, false);
		}finally{
			IOUtils.closeStream(in);
		}
	}
}
