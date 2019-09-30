package transactionsmapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;


public class MapReduceBaseConfigurator {

       /**  Configure method to simplify the repetitive work for MapReduce on main methods */

       public static <T extends Class, T2 extends Class<? extends Mapper>, T3 extends Class<? extends Reducer>, T4 extends Class, T5 extends Class, T6 extends Class, T7 extends Class>
       void configure(T jarByClass, T2 mapperClass, T3 reducerClass, T4 mapOutputKeyClass, T5 mapOutputValueClass, T6 outputKeyClass, T7 outputValueClass, String[] args) throws Exception {
              BasicConfigurator.configure();

              Configuration c = new Configuration();
              String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

              // arquivo de entrada
              Path input = new Path(files[0]);

              // arquivo de saida
              Path output = new Path(files[1]);

              // criacao do job e seu nome
              Job j = new Job(c, "transaction");

              // registrando as classes
              j.setJarByClass(jarByClass);
              j.setMapperClass(mapperClass);
              j.setReducerClass(reducerClass);

              // tipos de saida do mapper
              j.setMapOutputKeyClass(mapOutputKeyClass);
              j.setMapOutputValueClass(mapOutputValueClass);

              // tipo de saida do reducer
              j.setOutputKeyClass(outputKeyClass);
              j.setOutputValueClass(outputValueClass);

              // Paths de entrada e saida com o job
              FileInputFormat.addInputPath(j, input);
              FileOutputFormat.setOutputPath(j, output);

              System.exit(j.waitForCompletion(true) ? 0 : 1);
       }

}

