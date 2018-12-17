package cn.gaohank.program.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsDemoTest {
    private Configuration conf = null;
    private FileSystem fs = null;

    @Before
    public void init() throws Exception {
        conf = new Configuration();

        // 在core-site.xml中配置
        // 指定了默认的文件系统和namenode的地址
        conf.set("fs.defaultFS", "hdfs://hadoop:9000");
        fs = FileSystem.get(new URI("hdfs://hadoop:9000"), conf, "root");
    }

    public FileSystem getFileSystem() throws IOException, InterruptedException, URISyntaxException {
        return FileSystem.get(new URI("hdfs://hadoop:9000"), conf, "root");
    }

    @Test
    public void upLoad() throws Exception {
        FileSystem fs = getFileSystem();
        fs.copyFromLocalFile(new Path("./hdfs/in/demo.txt"), new Path("/hdfsdemo.txt"));
        fs.close();
    }

    @Test
    public void downLoad() throws Exception {
        FileSystem fs = getFileSystem();
        fs.copyToLocalFile(new Path("/hdfsdemo.txt"), new Path("./hdfs/out/demo1.txt"));
        fs.copyToLocalFile(false, new Path("/hdfsdemo.txt"), new Path("./hdfs/out/demo2.txt"), true);
        fs.copyToLocalFile(false, new Path("/hdfsdemo.txt"), new Path("./hdfs/out/demo3.txt"), false);
        // true表示删除源文件
        fs.copyToLocalFile(true, new Path("/hdfsdemo.txt"), new Path("./hdfs/out/demo4.txt"), false);
        fs.copyToLocalFile(true, new Path("/hdfsdemo.txt"), new Path("./hdfs/out/demo5.txt"), true);
        fs.close();
    }

    @Test
    public void mkdir() throws Exception {
        FileSystem fs = getFileSystem();
        fs.mkdirs(new Path("/aaa/bbb"));
        fs.close();
    }

    @Test
    public void delMulu() throws Exception {
        FileSystem fs = getFileSystem();
        fs.delete(new Path("/aaa"), true);
        fs.close();
    }

    @Test
    public void makdirTest() throws Exception {
        FileSystem fs = getFileSystem();
        boolean mkdirs = fs.mkdirs(new Path("/aaa/bbb"));
        System.out.println(mkdirs);
    }

    @Test
    public void deleteTest() throws Exception {
        FileSystem fs = getFileSystem();
        boolean delete = fs.delete(new Path("/aaa"), true);// true， 递归删除
        System.out.println(delete);
    }

    @Test
    public void listTest() throws Exception {
        FileSystem fs = getFileSystem();

        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : listStatus) {
            System.err.println(fileStatus.getPath() + "=================" + fileStatus.toString());
        }
        // 会递归找到所有的文件
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus next = listFiles.next();
            String name = next.getPath().getName();
            Path path = next.getPath();
            System.out.println(name + "---" + path.toString());
        }
    }

    @Test
    public void listFiles() throws Exception {
        FileSystem fs = getFileSystem();

        // RemoteIterator 远程迭代器
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while (listFiles.hasNext()) {
            LocatedFileStatus file = listFiles.next();
            Path path = file.getPath();
            System.out.println(path.toString());
            System.out.println("权限：" + file.getPermission());
            System.out.println("组：" + file.getGroup());
            System.out.println("文件大小：" + file.getBlockSize());
            System.out.println("所属者：" + file.getOwner());
            System.out.println("副本数：" + file.getReplication());

            BlockLocation[] blockLocations = file.getBlockLocations();
            for (BlockLocation bl : blockLocations) {
                System.out.println("块起始位置：" + bl.getOffset());
                System.out.println("块长度：" + bl.getLength());
                String[] hosts = bl.getHosts();
                for (String h : hosts) {
                    System.out.println("块所在DataNode：" + h);
                }
            }

            System.out.println("*****************************************");
        }

        System.out.println("-----------------分割线----------------");

        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        for (FileStatus status : listStatus) {
            String name = status.getPath().getName();
            System.out.println(name + (status.isDirectory() ? " is Dir" : " is File"));
        }
    }
}