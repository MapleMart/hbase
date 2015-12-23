package com.hyf.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.hyf.hbase.util.HBaseUtil;

/**
 * hbase练习:1、创建表 2、删除表 3、插入一行记录 4、删除一行记录 5、查找一行记录 6、显示所有数据 7、HBase批量处理的方法
 * 把hbase-0.98.16.1-hadoop2\lib 目录下的jar包全复制到工程里
 * @author 黄永丰
 * @createtime 2015年12月21日
 * @version 1.0
 */
public class MyHBase
{

	/**
	 * 创建一张表
	 * @author 黄永丰
	 * @createtime 2015年12月21日
	 * @param tableName 表名
	 * @param familys 列族
	 * @throws Exception
	 */
	public static void creatTable(String tableName, String[] familys) throws Exception
	{
		HBaseAdmin admin = new HBaseAdmin(HBaseUtil.config);
		if (admin.tableExists(tableName))
		{
			System.out.println("table already exists!");
		}
		else
		{
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			for (int i = 0; i < familys.length; i++)
			{
				tableDesc.addFamily(new HColumnDescriptor(familys[i]));
			}
			admin.createTable(tableDesc);
			System.out.println("create table " + tableName + " ok.");
		}
	}

	/**
	 * 删除表
	 * @author 黄永丰
	 * @createtime 2015年12月21日
	 * @param tableName 表名称
	 * @throws Exception
	 */
	public static void deleteTable(String tableName) throws Exception
	{
		try
		{
			HBaseAdmin admin = new HBaseAdmin(HBaseUtil.config);
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			System.out.println("delete table " + tableName + " ok.");
		}
		catch (MasterNotRunningException e)
		{
			e.printStackTrace();
		}
		catch (ZooKeeperConnectionException e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * 插入一行记录
	 * @author 黄永丰
	 * @createtime 2015年12月21日
	 * @param tableName 表名称
	 * @param rowKey 行名称
	 * @param family 列族
	 * @param qualifier 列族里的字段
	 * @param value 列族里的字段的值
	 * @throws Exception
	 */
	public static void addRecord(String tableName, String rowKey, String family, String qualifier, String value) throws Exception
	{
		try
		{
			HTable table = new HTable(HBaseUtil.config, tableName);
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
			table.put(put);
			System.out.println("insert recored " + rowKey + " to table " + tableName + " ok.");
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * 插入多行记录(HBase批量处理的方法)
	 * @author 黄永丰
	 * @createtime 2015年12月21日
	 * @param tableName 表名称
	 * @param puts 记录数组{ rowKey 行名称,family 列族,qualifier 列族里的字段,value 列族里的字段的值}
	 * @throws Exception
	 */
	public static void addBatchRecord(String tableName, List<Put> puts) throws Exception
	{
		try
		{
			HTablePool pool = new HTablePool(HBaseUtil.config, 1000);
			HTableInterface table = pool.getTable(tableName);
			table.setAutoFlush(false);
			table.setWriteBufferSize(5);
			table.put(puts);
			for (Put put : puts)
			{
				System.out.println("insert recored " +put.getFamilyMap() + " to table " + tableName + " ok.");
			}
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * 删除一行记录
	 * @author 黄永丰
	 * @createtime 2015年12月21日
	 * @param tableName 表名称
	 * @param rowKey 行名称
	 * @throws IOException
	 */
	public static void delRecord(String tableName, String rowKey) throws IOException
	{
		HTable table = new HTable(HBaseUtil.config, tableName);
		List list = new ArrayList();
		Delete del = new Delete(rowKey.getBytes());
		list.add(del);
		table.delete(list);
		System.out.println("del recored " + rowKey + " ok.");
	}

	/**
	 * 查找一行记录
	 * @author 黄永丰
	 * @createtime 2015年12月21日
	 * @param tableName 表名称
	 * @param rowKey 行名称
	 * @throws IOException
	 */
	public static void getOneRecord(String tableName, String rowKey) throws IOException
	{
		HTable table = new HTable(HBaseUtil.config, tableName);
		Get get = new Get(rowKey.getBytes());
		Result rs = table.get(get);
		for (KeyValue kv : rs.raw())
		{
			System.out.print(new String(kv.getRow()) + " ");
			System.out.print(new String(kv.getFamily()) + ":");
			System.out.print(new String(kv.getQualifier()) + " ");
			System.out.print(kv.getTimestamp() + " ");
			System.out.println(new String(kv.getValue()));
		}
	}

	/**
	 * 显示所有数据
	 * @author 黄永丰
	 * @createtime 2015年12月21日
	 * @param tableName 表名称
	 */
	public static void getAllRecord(String tableName)
	{
		try
		{
			HTable table = new HTable(HBaseUtil.config, tableName);
			Scan s = new Scan();
			ResultScanner ss = table.getScanner(s);
			for (Result r : ss)
			{
				for (KeyValue kv : r.raw())
				{
					System.out.print(new String(kv.getRow()) + " ");
					System.out.print(new String(kv.getFamily()) + ":");
					System.out.print(new String(kv.getQualifier()) + " ");
					System.out.print(kv.getTimestamp() + " ");
					System.out.println(new String(kv.getValue()));
				}
			}
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	// 测试
	public static void main(String[] agrs)
	{
		try
		{
			String tableName = "webpage";
			String[] familys = { "meta", "info" };
			String rowKey = "www.ithold.cn";
			String rowKey2 = "www.maplemart.com.cn";
			String[] qualifier = { "meta:title", "meta:summary", "meta:data", "info:info" };

			// create table
			// MyHBase.creatTable(tableName, familys);

			// delete record
			// MyHBase.deleteTable(tableName);

			// add record
			// MyHBase.addRecord(tableName, rowKey, familys[0],qualifier[0], "ithold官网");
			// MyHBase.addRecord(tableName, rowKey, familys[0],qualifier[1], "ithold开启大数据课程");
			// MyHBase.addRecord(tableName, rowKey, familys[0],qualifier[2], "2014-5-23");
			// MyHBase.addRecord(tableName, rowKey, familys[1],qualifier[3],
			// "<html><head></head><body>xxxxxxxxxxx</body></html");

			// MyHBase.addRecord(tableName, rowKey2, familys[0],qualifier[0], "maplemart官网");
			// MyHBase.addRecord(tableName, rowKey2, familys[0],qualifier[1], "maplemart开启大数据课程");
			// MyHBase.addRecord(tableName, rowKey2, familys[0],qualifier[2], "2014-5-25");
			// MyHBase.addRecord(tableName, rowKey2, familys[1],qualifier[3],
			// "<html><head></head><body>maplemart</body></html");

			// System.out.println("===========get one record========");
			// MyHBase.getOneRecord(tableName, rowKey);

			 System.out.println("===========show all record========");
			 MyHBase.getAllRecord(tableName);

			// System.out.println("===========del one record========");
			// MyHBase.delRecord(tableName, rowKey2);
			// MyHBase.getAllRecord(tableName);

			// addBatchRecord
			List<Put> puts = new ArrayList<Put>();
			for (int i = 0; i < 10; i++)
			{
				Put put = new Put(Bytes.toBytes(rowKey2 + "/" + i));
				put.add(Bytes.toBytes(familys[0]), Bytes.toBytes(qualifier[0]), Bytes.toBytes("maplemart官网"));
				puts.add(put);
			}
			System.out.println(puts.size());
			MyHBase.addBatchRecord(tableName, puts);
			MyHBase.getAllRecord(tableName);

		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

}
