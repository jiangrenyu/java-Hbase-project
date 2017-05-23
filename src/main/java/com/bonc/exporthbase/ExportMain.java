package com.bonc.exporthbase;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class ExportMain {
	public static void main(String args[]) throws ParseException, IOException {
		Options options = new Options();
		Option option = new Option("c", "confPath", true, "confFilePath");
		options.addOption(option);
		Parser parser = new PosixParser();
		CommandLine commandLine = parser.parse(options, args, true);
		Configuration conf = HBaseConfiguration.create();
		conf.addResource("hdfs-site.xml");
		conf.addResource("hbase-site.xml");
		conf.addResource(new Path(commandLine.getOptionValue('c')));
   
		ReadFromHbase eh = new ReadFromHbase(conf);
		eh.readFromHbase();
	}
}
