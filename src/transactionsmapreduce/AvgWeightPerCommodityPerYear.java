package transactionsmapreduce;

import org.apache.hadoop.io.FloatWritable;


/** PROBLEMA 4: Média de peso por mercadoria, separadas de acordo com o ano*/

public class AvgWeightPerCommodityPerYear {

    public static void main(String[] args) throws Exception {

        MapReduceBaseConfigurator.configure(
                AvgWeightPerCommodityPerYear.class, //jarByClass
                AvgWeightPerCommodityPerYearMapper.class, //mapperClass
                AvgFloatReducer.class, //reducerClass
                IntStringCompositeKeyWritable.class, //mapOutputKeyClass
                IntFloatWritable.class, //mapOutputValueClass
                IntStringCompositeKeyWritable.class, //outputKeyClass
                FloatWritable.class, //outputValueClass
                args
        );

    }

}
