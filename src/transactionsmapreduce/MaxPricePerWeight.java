package transactionsmapreduce;

import org.apache.hadoop.io.Text;

/** PROBLEMA 8: Mercadoria com o maior preço por unidade de peso*/

public class MaxPricePerWeight {

    public static void main(String[] args) throws Exception {

        MapReduceBaseConfigurator.configure(
                MaxPricePerWeight.class, //jarByClass
                MaxPricePerWeightMapper.class, //mapperClass
                MaxFloatWithIdReducer.class, //reducerClass
                Text.class, //mapOutputKeyClass
                IntFloatWritable.class, //mapOutputValueClass
                Text.class, //outputKeyClass
                IntFloatWritable.class, //outputValueClass
                args
        );

    }

}
