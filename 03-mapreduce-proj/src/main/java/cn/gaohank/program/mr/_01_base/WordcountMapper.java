package cn.gaohank.program.mr._01_base;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * KEYIN:默认情况下是mr框架所读到的一行文本的起始偏移量 long
 * 不过我们在这里采用的是hadoop给你的LongWritable
 * valuein：默认情况是mr框架所读到的一行文本的内容 string 用Text
 * KEYOUT：用户自定义逻辑处理完成后输出数据中的key。srting
 * VALUEOUT：用户自定义处理完成后输出的valuer。Integer IntWritable
 * 	  
 */

public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	/**
	 * map阶段的业务逻辑就应该写在map（）方法中
	 * maptask会对每一次输入的用map方法处理
	 */
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		//将传递进来的内容转换成String
		String line = value.toString();//hello shipeng
		//用空格切分数据
		String[] words = line.split(" ");//hello,shipeng,hello,feima....
		
		//将单词输出为<单词，1>
		for(String word:words){
			//将单词做为key 1作为value一边与以后的分发 发到reduce
			context.write(new Text(word),new IntWritable(1));
		}
		
		
	}

}
