package transactionsmapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountIntReducer<T extends Writable> extends Reducer<T, IntWritable, T, IntWritable> {

    public void reduce(T key,
                       Iterable<IntWritable> values,
                       Context context) throws IOException, InterruptedException {

        int count = 0;

        for(IntWritable val : values){
            count += 1;
        }

        IntWritable event_count = new IntWritable(count);


        // write the result
        context.write(key, event_count);
    }
}


