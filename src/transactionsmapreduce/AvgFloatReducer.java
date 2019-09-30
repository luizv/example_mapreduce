package transactionsmapreduce;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AvgFloatReducer<T extends Writable> extends Reducer<T, IntFloatWritable, T, FloatWritable> {

    public void reduce(T key,
                       Iterable<IntFloatWritable> values,
                       Context context) throws IOException, InterruptedException {

        int n = 0;
        float sum = 0;

        for(IntFloatWritable val : values){
                sum += val.getValue();
                n += val.getN();
        }

        FloatWritable average = new FloatWritable(sum/n);

        // writing on context
        context.write(key, average);
    }
}


