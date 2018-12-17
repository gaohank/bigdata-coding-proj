package cn.gaohank.program.mr._02_flow;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowCount {
	static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// 将一行数据转化成String
			String line = value.toString();
			// 切分字符
			String fields[] = line.split("\t");
			// 手机号
			String phoneNbr = fields[1];
			// 取出上下行流量
			long upFlow = Long.parseLong(fields[fields.length - 3]);
			long dFlow = Long.parseLong(fields[fields.length - 2]);
			
			context.write(new Text(phoneNbr), new FlowBean(upFlow, dFlow));
		}
		
	}
	
	static class FlowCountReaducer extends Reducer<Text, FlowBean, Text, FlowBean>{

		@Override
		protected void reduce(Text key, Iterable<FlowBean> values, Context context)
				throws IOException, InterruptedException {
			long sum_upFlow = 0;
			long sum_dFlow = 0;
			
			// 遍历所有bean，将上行和下行流量分别累加
			for (FlowBean bean:values) {
				sum_upFlow += bean.getUpFlow();
				sum_dFlow += bean.getdFlow();
			}
			
			// 将所有流量进行汇总
			FlowBean resultBean = new FlowBean(sum_upFlow, sum_dFlow);
			context.write(key, resultBean);
		}
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		/*conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resoucemanager.hostname", "mini1");*/
		conf.set("mapreduce.framework.name", "local");
		conf.set("yarn.resoucemanager.hostname","local");
		conf.set("fs.defaultFS", "file:///");
		Job job = Job.getInstance(conf);
		
		/*job.setJar("/home/hadoop/wc.jar");*/
		//指定本程序的jar包所在的本地路径
		job.setJarByClass(FlowCount.class);
		
		//指定本业务job要使用的mapper/Reducer业务类
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReaducer.class);
		
		//指定mapper输出数据的kv类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		
		//指定最终输出的数据的kv类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		//指定job的输入原始文件所在目录
		FileInputFormat.setInputPaths(job, new Path("./mapreduce/flow/in"));
		//指定job的输出结果所在目录
		FileOutputFormat.setOutputPath(job, new Path("./mapreduce/flow/out"));
		
		//将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
		/*job.submit();*/
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:1);
	}
}
