package cn.gaohank.program.mr._01_base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 相当于yarn集群的客户端
 * 需要再次封装我们的mr程序运行的相关参数和jar包
 * 最后提交给yarn--->hadoop集群
 */

public class WordcountDriver {
	//必须要main方法，因为只有这个程序开始后才能后面的处理
	public static void main(String[] args) throws Exception {
		
		//初始配置
		Configuration conf = new Configuration();
		// 在linux上，使用hadoop jar xxx.jar执行
//		conf.set("mapreduce.framework.name", "yarn");
//		conf.set("yarn.resoucemanager.hostname", "hadoop");
		// 在windows上执行
		conf.set("mapreduce.framework.name", "local");
		conf.set("yarn.resoucemanager.hostname","local");
		conf.set("fs.defaultFS", "file:///");		
		Job job = Job.getInstance(conf);
		 
		//指定本程序JAR报的路径
		job.setJarByClass(WordcountDriver.class);
		//指定本业务job要使用的map业务类
		job.setMapperClass(WordcountMapper.class);
		//指定本业务job要使用的reduce业务类
		job.setReducerClass(WordcountReducer.class);
		//指定map输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//最终reduce输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//指定job输入源文件所在目录
		FileInputFormat.setInputPaths(job, new Path("./mapreduce/wordcount/in"));
		//指定job输出结果
		FileOutputFormat.setOutputPath(job, new Path("./mapreduce/wordcount/out"));
		//将job中配置的相关参数以及先关job所用的java的jar包交给yarn
		//job.submit();
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:1);
		
	}

}