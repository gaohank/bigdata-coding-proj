package cn.gaohank.program.mr._01_base;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * KEYIN, VALUEIN和 Mapper输出的对应
 * KEYOUT是单词
 * VALUEOUT总次数
 */

public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		
		int count = 0;
	
		for(IntWritable value:values){
			count += value.get();
		}
		context.write(key, new IntWritable(count));
		//后面有个组件拿到你的count输出到一个文件中，有几个reducetast写几个。一般写到hdfs中
		//同时你任务开始的文件也需要被指定
	}

}
