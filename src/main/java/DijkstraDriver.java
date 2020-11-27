import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DijkstraDriver {

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        //设置主机地址及端口号

        conf.set("fs.defaultFS", Constant.hdfsSrc);
        try {
            FileSystem fs = FileSystem.get(conf);
            int i = 0;
            long num = 1;
            long tmp = 0;
            while (num > 0) {
                i++;
                conf.setInt("run.counter", i);
                //conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, "\t");
                Job job = Job.getInstance(conf);
                job.setJarByClass(DijkstraDriver.class);
                job.setMapperClass(DijkstraMapper.class);
                job.setReducerClass(DijkstraReducer.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Node.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                //key value 的格式   第一个item为key，后面的item为value
                job.setInputFormatClass(KeyValueTextInputFormat.class);
                //设置输入、输出路径

                if (i == 1)
                    FileInputFormat.addInputPath(job, new Path(Constant.inputSrc));
                else
                    FileInputFormat.addInputPath(job, new Path(Constant.outputSrc + (i - 1)));
                Path outPath = new Path(Constant.outputSrc + i);
                if (fs.exists(outPath)) {
                    fs.delete(outPath, true);
                }
                FileOutputFormat.setOutputPath(job, outPath);
                boolean b = job.waitForCompletion(true);
                if (b) {
                    num = job.getCounters().findCounter(eInf.COUNTER).getValue();
                    if (num == 0) {
                        System.out.println("共执行了" + i + "次，完成最短路径计算");
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
