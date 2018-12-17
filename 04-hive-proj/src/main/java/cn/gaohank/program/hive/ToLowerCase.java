package cn.gaohank.program.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

public class ToLowerCase extends UDF {
	public String evaluate(String field) {
		String result = field.toLowerCase();
		return result;
	}
}