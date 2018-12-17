package cn.gaohank.program.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.fasterxml.jackson.databind.ObjectMapper;



public class JsonParser extends UDF {

	public String evaluate(String jsonLine) {

		ObjectMapper objectMapper = new ObjectMapper();

		try {
			//解析数据
			MovieRateBean bean = objectMapper.readValue(jsonLine, MovieRateBean.class);
			return bean.toString();
		} catch (Exception e) {

		}
		return "";
	}

}
