package cn.gaohank.program.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsDemo {
    	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		// 拿到一个文件系统操作的客户端实例对象
		FileSystem fs = FileSystem.get(conf);

		fs.copyFromLocalFile(new Path("G:/access.log"), new Path("/access.log.copy"));
		fs.close();
	}
}
