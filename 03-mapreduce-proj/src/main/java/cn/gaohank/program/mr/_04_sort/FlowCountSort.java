package cn.gaohank.program.mr._04_sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 13480253104 180 180     360 
 * 13502468823 7335 110349 117684 
 * 13560436666 1116 954    2070
 */
public class FlowCountSort {

	static class FlowCountSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    //由于以前每次调用都要重新new一个，所以我们这次直接把他new出来，然后直接带进去
		//流量
		FlowBean bean = new FlowBean();
		//手机号
		Text v = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// 拿到的是上一个统计程序的输出结果，已经是各手机号的总流量信息
			String line = value.toString();

			String[] fields = line.split("\t");

			String phoneNbr = fields[0];

			long upFlow = Long.parseLong(fields[1]);
			long dFlow = Long.parseLong(fields[2]);
      //这里原来要一个一个的set我们在FlowBean里写一个一次性set
			bean.set(upFlow, dFlow);
			v.set(phoneNbr);
      /**
        *这里需要特殊说明，虽然每次都要输出同一个项，但是在hadoop序列化传递过程中，
        *map每改一个值都要重新写一个对应的值到文件中，所以每次都不一样
        *而reduce是反序列化所以每次取到的值也是不同的。
        */
			context.write(bean, v);

		}

	}

	/**
	 * 根据key来调, 传过来的是对象, 每个对象都是不一样的, 所以每个对象都调用一次reduce方法
	 */
	static class FlowCountSortReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

		// <bean(),phonenbr>
		@Override
		protected void reduce(FlowBean bean, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			context.write(values.iterator().next(), bean);

		}

	}
	
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		
	    conf.set("mapreduce.framework.name", "local");
		conf.set("yarn.resoucemanager.hostname","local");
		conf.set("fs.defaultFS", "file:///");

		Job job = Job.getInstance(conf);
		
		
		//指定本程序的jar包所在的本地路径
		job.setJarByClass(FlowCountSort.class);
		
		//指定本业务job要使用的mapper/Reducer业务类
		job.setMapperClass(FlowCountSortMapper.class);
		job.setReducerClass(FlowCountSortReducer.class);
		// 在map端进行合并，避免数据倾斜的问题
		// 如果combiner影响处理结构，就不加入
		job.setCombinerClass(FlowCountSortReducer.class);
		
		//指定mapper输出数据的kv类型
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);
		
		//指定最终输出的数据的kv类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		//指定job的输入原始文件所在目录
		FileInputFormat.setInputPaths(job, new Path("./mapreduce/part/out"));
		//指定job的输出结果所在目录
		
		Path outPath = new Path("./mapreduce/sort/out");
		
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outPath)){
			fs.delete(outPath, true);
		}
		FileOutputFormat.setOutputPath(job, outPath);
		
		//将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
		/*job.submit();*/
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:1);
		
	
	}
}