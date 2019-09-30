package transactionsmapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/** PROBLEMA 2: Quantidade de transações financeiras realizadas por ano  */

public class TransactionsPerYear {

    public static void main(String[] args) throws Exception {
        MapReduceBaseConfigurator.configure(
                TransactionsPerYear.class, //jarByClass
                TransactionsPerYearMapper.class, //mapperClass
                CountIntReducer.class, //reducerClass
                Text.class, //mapOutputKeyClass
                IntWritable.class, //mapOutputValueClass
                Text.class, //outputKeyClass
                IntWritable.class, //outputValueClass
                args
        );

    }

}
