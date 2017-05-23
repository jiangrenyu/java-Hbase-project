package com.bonc.exporthbase;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.mortbay.jetty.servlet.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadFromHbase {
	private Table table;
	private String tableName;// 表名
	private String rowKey; // 行健
	private String family;
	private String sperator; // 分隔符
	private String outputPath; // 输出路径
	private boolean isHDFS;
	private FileSystem fs;
	private static final Logger LOG = LoggerFactory.getLogger(ReadFromHbase.class);
	private String[] hbaseFields;

	public ReadFromHbase(Configuration conf) {
		this.tableName = conf.get("conf.HBase.TableName");// 表名
		boolean krbUse = conf.getBoolean("mr.use.kerberos", true);
		try {
			if (!krbUse) {
				fs = FileSystem.get(conf);
			} else {
				String krbKeystore = conf.get("mr.kerberos.ketsotre");
				String krbPrincpal = conf.get("mr.kerberos.princpal");
				conf.set("hadoop.security.authentication", "Kerberos");
				UserGroupInformation.setConfiguration(conf);
				UserGroupInformation.loginUserFromKeytab(krbPrincpal, krbKeystore);
				fs = (DistributedFileSystem) FileSystem.get(conf);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}

		try {
			Connection connection = ConnectionFactory.createConnection(conf);
			this.table = connection.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			e.printStackTrace();
		}

		this.rowKey = conf.get("conf.hbase.rowKey"); // rowKey字符串
		this.family = conf.get("conf.HBase.family");
		this.sperator = conf.get("conf.data.Sperator");
		this.outputPath = conf.get("conf.export.path");
		this.isHDFS = conf.getBoolean("conf.Hbase.isHDFS", true); // 是否写入HDFS

		this.hbaseFields = conf.get("conf.hbase.fields").split(",");
	}

	public void readFromHbase() throws IOException {

		OutputStream fos;
		if (isHDFS) {
			fos = fs.create(new Path(outputPath));
		} else {
			fos = new FileOutputStream(outputPath);
		}

		String rowKeys[] = rowKey.toString().split(",", -1);

		if (rowKeys.length == 0) {
			try {
				throw new Exception();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(family));
		ResultScanner scanner;
		if (rowKeys[0].equals("ALL")) {
			// 全表扫描
			scanner = table.getScanner(scan);
			for (Result result : scanner) {
				fos.write(getLine(result).getBytes());
			}
			fos.close();
		} else if ((rowKeys.length > 1) && rowKeys[1].equals("-")) {
			// 根据rowKey范围查询
			scan.setStartRow(Bytes.toBytes(rowKeys[0]));
			scan.setStopRow(Bytes.toBytes(rowKeys[2]));
			scanner = table.getScanner(scan);
			for (Result result : scanner) {
				fos.write(getLine(result).getBytes());
			}
			fos.close();

		} else if (rowKeys.length >= 1) {
			// 查询特定的rowKey
			for (String s : rowKeys) {
				Get get = new Get(Bytes.toBytes(s));
				Result result = table.get(get);
				fos.write(getLine(result).getBytes());
			}
			fos.close();
		}
	}

	private String getLine(Result result) {
		HashMap<String, String> map = new HashMap<String, String>();
		StringBuffer sb = new StringBuffer();
		String line = "";

		for (Cell cell : result.rawCells()) {
			String field = new String(CellUtil.cloneQualifier(cell));
			String value = new String(CellUtil.cloneValue(cell));
			map.put(field, value);
		}
		for (String s : hbaseFields) {
			if (map.containsKey(s)) {
				sb.append(map.get(s));
			} else {
				sb.append("");
			}
			sb.append(this.sperator);
		}

		if (sb.length() > 1) {
			line = sb.deleteCharAt(sb.length() - 1).toString() + "\r\n";
		} else {
			LOG.error("This table {} is not found in HBase {};", tableName);
		}
		return line;
	}
}
