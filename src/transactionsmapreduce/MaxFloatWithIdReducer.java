package transactionsmapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxFloatWithIdReducer extends Reducer<Text, IntFloatWritable, Text, IntFloatWritable> {

    public void reduce(Text key,
                       Iterable<IntFloatWritable> values,
                       Context context) throws IOException, InterruptedException {

        float max_float = Float.MIN_VALUE;
        int id = 0;

        for(IntFloatWritable val : values){
            if (val.getValue() > max_float) {
                max_float = val.getValue();
                id = val.getN();
            }
        }

        IntFloatWritable result = new IntFloatWritable(id, max_float);

        // escrevendo o resultado
        context.write(key, result);
    }
}


