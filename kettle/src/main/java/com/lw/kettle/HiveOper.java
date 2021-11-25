package com.lw.kettle;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * @author lw
 * @date 2021/8/24 0024
 * @description
 */
@Slf4j
public class HiveOper {

    @Before
    public void init(){
        System.setProperty("HADOOP_USER_NAME","hadoop");
    }

    @Test
    public void test(){
        String name = "HADOOP_USER_NAME";
        String hadNameEnv = System.getenv(name);
        String hadNamePro = System.getProperty(name);
        System.out.println(hadNameEnv+"   "+hadNamePro);
    }

    /**
     * 通过写hive的文件系统，将数据录入进去
     */
    @Test
    public void writeHiveFile() throws SQLException, IOException {
        String randomId = UUID.randomUUID().toString().replace("-","");

        String tableName= "user_info_dest";

        String hdfsIpAndPort = "hdfs://192.168.10.211:9000";
        String dbPath = "/user/hive_remote/warehouse/ntzw_dev_64.db/";

        //建表语句后面的  location
        String filePath = hdfsIpAndPort+dbPath+tableName+"_"+randomId;

        //将数据写入Hadoop中
        this.writeData2Hadoop(filePath);

        //加载到hive表的语句  从hadoop文件中加载到hive库中，这样 select 语句才能看见
        String loadDataSql = "load data inpath '"+dbPath+tableName+"_"+randomId+"' "+"into table "+tableName;

        System.out.println(loadDataSql);

        //获取连接，并执行加载语句
        Connection connection = this.getHiveConnection();
        Statement stmt = connection.createStatement();
        stmt.execute(loadDataSql);

        stmt.close();
        connection.close();

        //加载完成后需要删除临时文件
        this.deleteHiveTmpFile(filePath);

    }

    /**
     * 往hadoop文件系统里面写文件
     * @param filePath
     * @throws IOException
     */
    private void writeData2Hadoop(String filePath) throws IOException {
        Configuration conf = new Configuration();
        conf.setBoolean("dfs.support.append", true);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        //获取文件系统对象
        FileSystem fs = FileSystem.get(URI.create(filePath),conf);

        OutputStream output = null;

        //文件检查
        Path path = new Path(filePath);
        if (!(fs.exists(path)&&fs.isFile(path))) {
            log.info("不存在文件[{}],开始创建文件.",filePath);
            fs.createNewFile(path);
            output = fs.create(path);
        } else {
            log.info("文件[{}]已存在,追加内容到文件",filePath);
            output = fs.append(path);
        }

        //往文件里面写数据
        List<UserInfoDest> dataList = this.initData();
        for (UserInfoDest data : dataList) {
            //目标表只有两列  id,name

            StringBuilder sb = new StringBuilder();
            sb.append(data.id);//表的id
            //hive的表的默认分割符，\t，如果指定了其他列分割符，那么此处也要改变
            sb.append("\001");
            sb.append(data.name);//表的name
            sb.append("\n");//一行内容结束后换行
            output.write(sb.toString().getBytes());
        }
        output.flush();
        output.close();

        fs.close();
    }

    /**
     * 删除Hive临时文件
     * @param filePath
     */
    private void deleteHiveTmpFile(String filePath){
        Configuration conf = new Configuration();
        conf.setBoolean("dfs.support.append", true);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        FileSystem fs = null;
        Path path = new Path(filePath);

        try{
            fs = FileSystem.get(URI.create(filePath), conf);

            if ((fs.exists(path)&&fs.isFile(path))) {
                fs.delete(path,false);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 获取hive的连接
     * @return
     */
    private Connection getHiveConnection(){
        String driver = "org.apache.hive.jdbc.HiveDriver";
        String url = "jdbc:hive2://192.168.10.212:10000/ntzw_dev_64";
        try {
            Class.forName(driver);
            Connection conn = DriverManager.getConnection(url,"hadoop","hadoop");
            return conn;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 初始化数据
     * @return
     */
    private List<UserInfoDest> initData(){
        String[] cols = new String[]{"id","name"};

        List<UserInfoDest> retList = new ArrayList<UserInfoDest>();

        //初始化两条数据
        for (int i = 0; i < 2; i++) {
            UserInfoDest dest = new UserInfoDest();
            dest.id = cols[0]+"_"+i;
            dest.name = cols[1]+"_"+i;
            retList.add(dest);
        }
        return retList;
    }
}

/**
 * hive的表对象
 */
class UserInfoDest{
    public String id;
    public String name;
}
