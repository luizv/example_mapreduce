package transactionsmapreduce;

import org.apache.hadoop.io.IntWritable;


/** PROBLEMA 9: Quantidade de transações comerciais de acordo com o fluxo, de acordo com o ano*/

public class TransactionsPerFlowPerYear {

    public static void main(String[] args) throws Exception {

        MapReduceBaseConfigurator.configure(
                TransactionsPerFlowPerYear.class, //jarByClass
                TransactionsPerFlowPerYearMapper.class, //mapperClass
                CountIntReducer.class, //reducerClass
                IntStringCompositeKeyWritable.class, //mapOutputKeyClass
                IntWritable.class, //mapOutputValueClass
                IntStringCompositeKeyWritable.class, //outputKeyClass
                IntWritable.class, //outputValueClass
                args
        );

    }

}
