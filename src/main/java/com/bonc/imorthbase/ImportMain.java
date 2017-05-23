package com.bonc.imorthbase;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;

public class ImportMain {
   public static void main (String [] args) throws ParseException, FileNotFoundException, IllegalArgumentException, IOException{
	    Options options = new Options();
		Option option = new Option("i", "inputPath", true, "inputPath");
		options.addOption(option);
		option = new Option("c", "confPath", true, "confFilePath");
		options.addOption(option);
		Parser parser = new PosixParser();
		CommandLine commandLine = parser.parse(options, args, true);
		Configuration conf = HBaseConfiguration.create();
		conf.addResource("hdfs-site.xml");
		conf.addResource("hbase-site.xml");
		conf.addResource(new Path(commandLine.getOptionValue('c')));
		
		FileSystem fs = null;
		boolean krbUse = Boolean.parseBoolean(conf.get("mr.use.kerberos"));
		String krbKeystore = conf.get("mr.kerberos.ketsotre");
		String krbPrincpal = conf.get("mr.kerberos.princpal");

		try {
			if (!krbUse) {
				fs = FileSystem.get(conf);
			} else {
				conf.set("hadoop.security.authentication", "Kerberos");
				UserGroupInformation.setConfiguration(conf);
				UserGroupInformation.loginUserFromKeytab(krbPrincpal, krbKeystore);
				fs = (DistributedFileSystem) FileSystem.get(conf);
			}

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
   
	   ImportToHbase it = new ImportToHbase(conf);
	   RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path(commandLine.getOptionValue('i')), true);
		    
		    while(iterator.hasNext()){
		    	FileStatus file =iterator.next();
		    	FSDataInputStream in = fs.open(file.getPath());
				BufferedReader br = new BufferedReader(new InputStreamReader(in));
				
				String line;      //接收每一行的值	
				while ((line = br.readLine()) != null) {
					it.importTo(line);
				}
		    }
		
	
   

   }
}
