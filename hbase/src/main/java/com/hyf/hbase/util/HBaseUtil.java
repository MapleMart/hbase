package com.hyf.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;

/**
 * HBase加载工具类
 * 把hbase-0.98.16.1-hadoop2\lib 目录下的jar包全复制到工程里
 * @author 黄永丰
 * @createtime 2015年12月21日
 * @version 1.0
 */
public class HBaseUtil
{
	public static Configuration config = null;
	static
	{
		config = HBaseConfiguration.create();
		//如果是win系统 ,要在C:\Windows\System32\drivers\etc下的hosts文件里加上
		//192.168.1.20 master
		//192.168.1.21 slave1
		//192.168.1.22 slave2
		//192.168.1.23 slave3
//		config.set("hbase.zookeeper.quorum", "slave1,slave2,slave3");
		config.set("hbase.zookeeper.quorum", "slave3");//这里我的zookeeper是伪分布式，所以就一个slave3,
		config.set("hbase.zookeeper.property.clientPort", "2181");
	}

	//下面测试创建表
	public static final String tableName = "webpage";
	public static final String colfmeta = "meta";//meta列族
	public static final String coltitle = "title";//meta列族里的标题
	public static final String colsummary = "summary";//meta列族里的摘要
	public static final String coldate = "date";//meta列族里的时间
	public static final String colfinfo = "info";//info列族
	public static final String colinfo = "info";//info列族里的内容
	public static String rowkey = "www.ithold.cn";//行名称

	public static void main(String[] args)
	{
		HTableInterface table = null;
		try
		{
			HBaseAdmin admin = new HBaseAdmin(config);
			if (admin.tableExists(tableName))
			{
				System.out.println("table is already exists!");
			}
			else
			{
				HTableDescriptor desc = new HTableDescriptor(tableName);
				HColumnDescriptor family = new HColumnDescriptor(colfmeta);
				desc.addFamily(family);
				family = new HColumnDescriptor(colfinfo);
				desc.addFamily(family);
				admin.createTable(desc);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
}
