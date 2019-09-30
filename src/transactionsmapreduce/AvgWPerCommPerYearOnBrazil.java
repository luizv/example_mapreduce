package transactionsmapreduce;

import org.apache.hadoop.io.FloatWritable;

public class AvgWPerCommPerYearOnBrazil {

    /** PROBLEMA 5: Média de peso por mercadoria comercializadas no Brasil, separadas de acordo com o ano*/

    public static void main(String[] args) throws Exception {

        MapReduceBaseConfigurator.configure(
                AvgWPerCommPerYearOnBrazil.class, //jarByClass
                AvgWPerCommPerYearOnBrazilMapper.class, //mapperClass
                AvgFloatReducer.class, //reducerClass
                IntStringCompositeKeyWritable.class, //mapOutputKeyClass
                IntFloatWritable.class, //mapOutputValueClass
                IntStringCompositeKeyWritable.class, //outputKeyClass
                FloatWritable.class, //outputValueClass
                args
        );

    }

}
