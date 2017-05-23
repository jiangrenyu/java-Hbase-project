package com.bonc.imorthbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class ImportToHbase {

	private String tableName;
	private Table table;
	private String family;
	private String rowKeys[];
	private String[] fields;
	private String[] columns;
	private String sperator;

	public ImportToHbase(Configuration conf) {
		this.tableName = conf.get("conf.HBase.TableName");
		try {
			Connection connection = ConnectionFactory.createConnection(conf);
			this.table = connection.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			e.printStackTrace();
		}

		this.family = conf.get("conf.HBase.familyName");
		this.sperator = conf.get("conf.mapreduce.dataSperator");
		this.rowKeys = conf.get("conf.hbase.rowKey").split(",", -1);
		this.fields = conf.get("conf.hdfs.fields").split(",");// 这个列建立映射
		this.columns = conf.get("conf.Hbase.qualifier").split(",");// 这个保证插入哪些列
		if (columns.equals("")) {
			throw new RuntimeException("conf.Hbase.qualifier is null");
		}
	}

	public void importTo(String line) throws IOException {
		// 将要插入的值转化为一个数组
		Map<String, String> map = getData(line);
		StringBuffer sb = new StringBuffer();
		for (String r : rowKeys) {
			sb.append(map.get(r));
		}
		String rowKey = sb.toString();
		// 行健
		Put put = new Put(Bytes.toBytes(rowKey));

		for (String s : columns) {
			String qualifiervalue = map.get(s) == null ? "" : map.get(s);
			put.addColumn(Bytes.toBytes(family), Bytes.toBytes(s), Bytes.toBytes(qualifiervalue));
		}
		table.put(put);
	}

	/*
	 * 获取列值和值得映射
	 */
	public Map<String, String> getData(String line) {
		Map<String, String> map = new HashMap<String, String>();
		String tmp[] = line.split(sperator, -1);
		int i = 0;
		for (String c : fields) {
			map.put(c, tmp[i]);
			i++;
		}
		return map;
	}

}
