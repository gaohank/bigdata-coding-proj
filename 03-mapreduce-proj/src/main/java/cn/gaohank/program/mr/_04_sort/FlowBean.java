package cn.gaohank.program.mr._04_sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
/**
 * WritableComparable<T>在这里这个T是需要比较的值，也就是咱们的hadoop的maptask阶段的key值
 * 这个key是keyout的key。在本例中特指代为flowbean的流量之和。
 * */
public class FlowBean implements WritableComparable<FlowBean>{
	
	private long upFlow;
	private long dFlow;
	private long sumFlow;
	
	//反序列化时，需要反射调用空参构造函数，所以要显示定义一个
	public FlowBean(){}
	
	public FlowBean(long upFlow, long dFlow) {
		this.upFlow = upFlow;
		this.dFlow = dFlow;
		this.sumFlow = upFlow + dFlow;
	}
	
	
	public void set(long upFlow, long dFlow) {
		this.upFlow = upFlow;
		this.dFlow = dFlow;
		this.sumFlow = upFlow + dFlow;
	}
	
	
	
	
	public long getUpFlow() {
		return upFlow;
	}
	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}
	public long getdFlow() {
		return dFlow;
	}
	public void setdFlow(long dFlow) {
		this.dFlow = dFlow;
	}


	public long getSumFlow() {
		return sumFlow;
	}


	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}


	/**
	 * 序列化方法
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(dFlow);
		out.writeLong(sumFlow);
		
	}


	/**
	 * 反序列化方法
	 * 注意：反序列化的顺序跟序列化的顺序完全一致
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		 upFlow = in.readLong();
		 dFlow = in.readLong();
		 sumFlow = in.readLong();
	}
	
	@Override
	public String toString() {
		 
		return upFlow + "\t" + dFlow + "\t" + sumFlow;
	}

	@Override
	public int compareTo(FlowBean o) {
	 //从大到小, 当前对象和要比较的对象比, 如果当前对象大, 返回-1, 交换他们的位置
		return this.sumFlow>o.getSumFlow()?1:-1;	
	}

}