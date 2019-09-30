package transactionsmapreduce;

import org.apache.hadoop.io.IntWritable;

/** PROBLEMA 1: Identificar mercadoria com maior quantidade de transações comerciais no Brasil */

public class TransactionsOnBrazil {

    public static void main(String[] args) throws Exception {
        MapReduceBaseConfigurator.configure(
                TransactionsOnBrazil.class, //jarByClass
                TransactionsOnBrazilMapper.class, //mapperClass
                CountIntReducer.class, //reducerClass
                IntStringCompositeKeyWritable.class, //mapOutputKeyClass
                IntWritable.class, //mapOutputValueClass
                IntStringCompositeKeyWritable.class, //outputKeyClass
                IntWritable.class, //outputValueClass
                args
        );
    }

}


