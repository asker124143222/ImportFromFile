import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * @Author: xu.dm
 * @Date: 2019/4/25 19:24
 * @Description:
 */
public class ImportFromFile {
//    private static String HDFSUri = "hdfs://bigdata-senior01.home.com:9000";
    public static final String NAME = "ImportFromFile";

    private static CommandLine parseArgs(String[] args) throws ParseException{
        Options options = new Options();

        Option option = new Option("t","table",true,"表不能为空");
        option.setArgName("table-name");
        option.setRequired(true);
        options.addOption(option);

        option = new Option("c","column",true,"列族和列名不能为空");
        option.setArgName("family:qualifier");
        option.setRequired(true);
        options.addOption(option);

        option = new Option("i","input",true,"输入文件或者目录");
        option.setArgName("path-in-HDFS");
        option.setRequired(true);
        options.addOption(option);

        options.addOption("d","debug",false,"switch on DEBUG log level");
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options,args);
        }catch (Exception e){
            System.err.println("ERROR: " + e.getMessage() + "\n");
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(NAME + " ", options, true);
            System.exit(-1);
        }
        if (cmd.hasOption("d")) {
            Logger log = Logger.getLogger("mapreduce");
            log.setLevel(Level.DEBUG);
        }

        return cmd;
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = HBaseConfiguration.create();

        String[] runArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
        CommandLine cmd = parseArgs(runArgs);
        if (cmd.hasOption("d")) conf.set("conf.debug", "true");

        String table = cmd.getOptionValue("t");
        String input = cmd.getOptionValue("i");
        String column = cmd.getOptionValue("c");
        conf.set("conf.column", column);

        Job job = Job.getInstance(conf,"Import from file " + input +" into table " + table);
        job.setJarByClass(ImportFromFile.class);
        job.setMapperClass(ImportMapper.class);
        job.setOutputFormatClass(TableOutputFormat.class);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE,table);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Writable.class);
        job.setNumReduceTasks(0); //不需要reduce

        FileInputFormat.addInputPath(job,new Path(input));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
