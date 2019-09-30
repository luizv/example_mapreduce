package transactionsmapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxIntReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key,
                       Iterable<IntWritable> values,
                       Context context) throws IOException, InterruptedException {

        int max_int = Integer.MIN_VALUE;

        for(IntWritable val : values){
            if (val.get() > max_int) {
                max_int = val.get();
            }
        }

        IntWritable event_count = new IntWritable(max_int);

        // escrevendo o resultado
        context.write(key, event_count);
    }
}


