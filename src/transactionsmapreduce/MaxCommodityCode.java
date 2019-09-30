package transactionsmapreduce;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/** PROBLEMA 7: Valor máximo de código*/

public class MaxCommodityCode {

    public static void main(String[] args) throws Exception {

        MapReduceBaseConfigurator.configure(
                MaxCommodityCode.class, //jarByClass
                MaxCommodityCodeMapper.class, //mapperClass
                MaxIntReducer.class, //reducerClass
                Text.class, //mapOutputKeyClass
                IntWritable.class, //mapOutputValueClass
                Text.class, //outputKeyClass
                IntWritable.class, //outputValueClass
                args
        );

    }

}
