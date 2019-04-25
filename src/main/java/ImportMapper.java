import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: xu.dm
 * @Date: 2019/4/25 19:26
 * @Description:
 */
public class ImportMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Mutation> {
    //计数器
    public enum Counters {
        LINES
    }

    private byte[] family = null;
    private byte[] qualifier = null;

    /**
     * Called once at the beginning of the task.
     *
     * @param context
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String column = context.getConfiguration().get("conf.column");
        ColParser parser = new ColParser();
        parser.parse(column);
        if(!parser.isValid()) throw new IOException("family or qualifier error");
        family = parser.getFamily();
        qualifier = parser.getQualifier();
    }

    /**
     * Called once for each key/value pair in the input split. Most applications
     * should override this, but the default is the identity function.
     *
     * @param key
     * @param value
     * @param context
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String line = value.toString();
            byte[] rowKey = DigestUtils.md5(line);
            Put put = new Put(rowKey);
            put.addColumn(this.family,this.qualifier,Bytes.toBytes(line));
            context.write(new ImmutableBytesWritable(rowKey),put);
            context.getCounter(Counters.LINES).increment(1);
        }catch (Exception e){
            e.printStackTrace();
        }

//        super.map(key, value, context);
    }

    class ColParser {
        private byte[] family;
        private byte[] qualifier;
        private boolean valid;

        public byte[] getFamily() {
            return family;
        }

        public byte[] getQualifier() {
            return qualifier;
        }

        public boolean isValid() {
            return valid;
        }

        public void parse(String value) {
            try {
                String[] sValue = value.split(":");
                if (sValue == null || sValue.length < 2 || sValue[0].isEmpty() || sValue[1].isEmpty()) {
                    valid = false;
                    return;
                }

                family = Bytes.toBytes(sValue[0]);
                qualifier = Bytes.toBytes(sValue[1]);
                valid = true;
            } catch (Exception e) {
                valid = false;
            }
        }


    }
}
