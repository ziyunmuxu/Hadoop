package cn.chapter3.demo1;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

/**
 * 第三章 3.5java接口，3.5.1从Hadoop URL读取数据
 * @author ziyunmuxu
 *
 * 重点：在hadoop环境是一定需要去掉包头，否则一直报错
 *
 */
public class URLCat {
	static {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	
	public static void main(String[] args) throws MalformedURLException, IOException {
		InputStream in = null;
		try{
			in = new URL(args[0]).openStream();
			IOUtils.copyBytes(in, System.out, 4096, false);
		}finally{
			IOUtils.closeStream(in);
		}
	}
}
