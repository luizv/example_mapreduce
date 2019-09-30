package transactionsmapreduce;

import org.apache.hadoop.io.IntWritable;


/** PROBLEMA 3: Mercadoria com maior quantidade de transações financeiras em 2016, no Brasil */

public class TransactionsPerYearOnBrazil {

    public static void main(String[] args) throws Exception {

        MapReduceBaseConfigurator.configure(
                TransactionsPerYearOnBrazil.class, //jarByClass
                TransactionsPerYearOnBrazilMapper.class, //mapperClass
                CountIntReducer.class, //reducerClass
                IntStringCompositeKeyWritable.class, //mapOutputKeyClass
                IntWritable.class, //mapOutputValueClass
                IntStringCompositeKeyWritable.class, //outputKeyClass
                IntWritable.class, //outputValueClass
                args
        );

    }

}
