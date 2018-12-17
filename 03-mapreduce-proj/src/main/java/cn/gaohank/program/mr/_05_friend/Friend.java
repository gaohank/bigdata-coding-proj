package cn.gaohank.program.mr._05_friend;

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
  
public class Friend {  
      
      
    public static class CommonFriendsOneMapper extends Mapper<LongWritable, Text, Text, Text> {  
  
        @Override  
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {  
              
            String line=value.toString();  
            String[] split=line.split(":");  
            String person=split[0];  
              
            String[] friends=split[1].split(",");  
              
            for(String f:friends){  
                context.write(new Text(f), new Text(person));  
              
            }  
        }  
    }  
    public static class CommonFriendsOneReducer extends Reducer<Text, Text, Text, Text> {  
  
        @Override  
        protected void reduce(Text friend, Iterable<Text> persons, Context context) throws IOException, InterruptedException {  
            StringBuffer sb = new  StringBuffer();  
              
            for(Text person:persons){  
                sb.append(person+",");  
            }  
            context.write(friend, new Text(sb.toString()));  
        }  
    }  
      
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {  
        //读取classpath下的所有xxx-site.xml配置文件，并进行解析  
                Configuration conf = new Configuration();
         
                conf.set("mapreduce.framework.name", "local");
                conf.set("yar.resoucemanager.hostname","local");
                conf.set("fs.defaultFS", "file:///");
                  
                Job wcjob = Job.getInstance(conf);  
                  
                //通过主类的类加载器机制获取到本job的所有代码所在的jar包  
                wcjob.setJarByClass(Friend.class);  
                  
                //指定本job使用的mapper类  
                wcjob.setMapperClass(CommonFriendsOneMapper.class);  
                //指定本job使用的reducer类  
                wcjob.setReducerClass(CommonFriendsOneReducer.class);  
                  
                  
                //指定reducer输出的kv数据类型  
                wcjob.setOutputKeyClass(Text.class);  
                wcjob.setOutputValueClass(Text.class);  
                  
                //指定本job要处理的文件所在的路径
                FileInputFormat.setInputPaths(wcjob, new Path("./mapreduce/friend/in/friend.txt"));  
                
                Path outPath = new Path("./mapreduce/friend/out");
        		
        		FileSystem fs = FileSystem.get(conf);
        		if(fs.exists(outPath)){
        			fs.delete(outPath, true);
        		}
                
                //指定本job输出的结果文件放在哪个路径  
                FileOutputFormat.setOutputPath(wcjob, new Path("./mapreduce/friend/out"));  
                  
                //将本job向hadoop集群提交执行  
                boolean res = wcjob.waitForCompletion(true);  
                  
                System.exit(res?0:1);  
  
    }  
  
}  
