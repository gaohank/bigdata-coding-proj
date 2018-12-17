package cn.gaohank.program.mr._06_db;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 订单表和商品表合到一起
 *order.txt(订单id, 日期, 商品编号, 数量)
 *	1001	20170327	P0001	2
 *  1002	20170327	P0001	3
 *  1002	20170327	P0002	3
 *  1003	20170327	P0003	3
 *product.txt(商品编号, 商品名字, 种类代码, 价格)
 *  P0001	希捷 10 700
 *  P0002 日立 10 500
 *  P0003 西数 10 600
 */
public class Join {
  
	static class JoinMapper extends Mapper<LongWritable, Text, Text, InfoBean> {
		InfoBean bean = new InfoBean();
		Text k = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      //先拿到文件行中的信息
			String line = value.toString();
      //由于我们的数据来自于文件，所以我们在这里需要定义切片类型为文件切片类型（FileSplit）代表强转换
			FileSplit inputSplit = (FileSplit) context.getInputSplit();
			//获取文件的路径和文件名 
			String name = inputSplit.getPath().getName();
			// 通过文件名判断是哪种数据
			String pid = "";
			// 通过文件名判断是哪种数据
			if (name.startsWith("order")) {
				//定义分隔符
				String[] fields = line.split("\t");
				pid = fields[2];
				/**
				*由于分隔符切分后 我们可以得到以下信息 以order文件为例
				*1001	20150710	P0001	2
				*上面数据所对应的字段是
				* id     date   pid  amount
				*所以可知 id为分隔符分割后的0号区 date是1号区 以此类推
				*/
				//set(int order_id,String dateString,String pid,int amount,String pname,int category_id,String flag)
				//为了防止空指针异常，所以给他们一个默认值
				bean.set(Integer.parseInt(fields[0]), fields[1], pid, Integer.parseInt(fields[3]), "", 0, 0, "0");

			} else {
				String[] fields = line.split("\t");
				// id pname category_id price
				pid = fields[0];
				//set(int order_id,String dateString,String pid,int amount,String pname,int category_id,String flag)
				bean.set(0, "", pid, 0, fields[1], Integer.parseInt(fields[2]), Float.parseFloat(fields[3]), "1");

			}
			k.set(pid);
			context.write(k, bean);
		}

	}

	static class JoinReducer extends Reducer<Text, InfoBean, InfoBean, NullWritable> {

		@Override
		protected void reduce(Text pid, Iterable<InfoBean> beans, Context context) throws IOException, InterruptedException {
			InfoBean pdBean = new InfoBean();
			ArrayList<InfoBean> orderBeans = new ArrayList<InfoBean>();

			for (InfoBean bean : beans) {
				if ("1".equals(bean.getFlag())) {	//产品的
					try {
						BeanUtils.copyProperties(pdBean, bean);
					} catch (Exception e) {
						e.printStackTrace();
					}
				} else {
					InfoBean odbean = new InfoBean();
					try {
						BeanUtils.copyProperties(odbean, bean);
						orderBeans.add(odbean);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}

			}

			// 拼接两类数据形成最终结果
			for (InfoBean bean : orderBeans) {

				bean.setPname(pdBean.getPname());
				bean.setCategory_id(pdBean.getCategory_id());
				bean.setPrice(pdBean.getPrice());

				context.write(bean, NullWritable.get());
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		conf.set("mapred.textoutputformat.separator", "\t");
		
		conf.set("mapreduce.framework.name", "local");
		conf.set("yarn.resoucemanager.hostname","local");
		conf.set("fs.defaultFS", "file:///");
		
		Job job = Job.getInstance(conf);

		// 指定本程序的jar包所在的本地路径
		// job.setJarByClass(Join.class);
       // job.setJar("c:/join.jar");

		job.setJarByClass(Join.class);
		// 指定本业务job要使用的mapper/Reducer业务类
		job.setMapperClass(JoinMapper.class);
		job.setReducerClass(JoinReducer.class);

		// 指定mapper输出数据的kv类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(InfoBean.class);

		// 指定最终输出的数据的kv类型
		job.setOutputKeyClass(InfoBean.class);
		job.setOutputValueClass(NullWritable.class);

		// 指定job的输入原始文件所在目录
		FileInputFormat.setInputPaths(job, new Path("./mapreduce/db/in"));
		// 指定job的输出结果所在目录
		FileOutputFormat.setOutputPath(job, new Path("./mapreduce/db/out"));

		// 将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
		/* job.submit(); */
		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);

	}
}