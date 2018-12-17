package cn.gaohank.program.mr._03_part;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * K2  V2  对应的是map输出kv的类型
 * @author
 *
 */
public class PhonePartitioner extends Partitioner<Text, FlowBean>{

	public static HashMap<String, Integer> phoneDict = new HashMap<String, Integer>();
	static{
		phoneDict.put("134", 0); 
		phoneDict.put("135", 0); 
		phoneDict.put("136", 0);
		phoneDict.put("137", 1);
		phoneDict.put("138", 2);
		phoneDict.put("139", 3);
	}
	
	
	
	@Override
	public int getPartition(Text key, FlowBean value, int numPartitions) {
		String prefix = key.toString().substring(0, 3);
		Integer phoneId = phoneDict.get(prefix);
		
		return phoneId==null?4:phoneId;
	}



}