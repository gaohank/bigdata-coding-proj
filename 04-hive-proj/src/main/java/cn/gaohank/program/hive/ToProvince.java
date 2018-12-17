package cn.gaohank.program.hive;

import java.util.HashMap;

import org.apache.hadoop.hive.ql.exec.UDF;

public class ToProvince extends UDF {
	public static HashMap<String, String> provinceMap = new HashMap<>();
	
	static {
		provinceMap.put("134", "shanghai");
		provinceMap.put("137", "shanxi");
		provinceMap.put("186", "gansu");
	}
	
	public String evaluate(String phonenbr) {
		String pnb = phonenbr;
		return provinceMap.get(pnb.substring(0,3)) == null ? "other" : provinceMap.get(pnb.substring(0,3));
	}
}

