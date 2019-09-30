package transactionsmapreduce;


import org.apache.hadoop.io.FloatWritable;

/** PROBLEMA 6: Média de valor por peso, de acordo com a mercadoria comercializadas no Brasil, separadas de acordo com o ano */

public class AvgPricePerWPerCommPerYOnBrazil {

    public static void main(String[] args) throws Exception {

        MapReduceBaseConfigurator.configure(
                AvgPricePerWPerCommPerYOnBrazil.class, //jarByClass
                AvgPricePerWPerCommPerYOnBrazilMapper.class, //mapperClass
                AvgFloatReducer.class, //reducerClass
                IntStringCompositeKeyWritable.class, //mapOutputKeyClass
                IntFloatWritable.class, //mapOutputValueClass
                IntStringCompositeKeyWritable.class, //outputKeyClass
                FloatWritable.class, //outputValueClass
                args
        );

    }

}
