package transactionsmapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TransactionsOnBrazilMapper extends Mapper<Object, Text, IntStringCompositeKeyWritable, IntWritable> {

    public void map(Object key, Text value, Context context) throws IOException,
            InterruptedException {

        // Prepare data
        String line = value.toString();
        String[] columns = line.split(";");

        // Check if it's first line, if it is, return
        if (columns[0].equals("country_or_area")) {
            return;
        }

        // Fix mismatch code. "9999AA" (not convertible to int) has the same meaning of "999999" (convertible to int).
        if (columns[2].equals("9999AA")) {
            columns[2] = "999999";
        }

        // Check if it's a valid product code number. If it isn't, return.
        int product_code_number;
        try {
            product_code_number = Integer.parseInt(columns[2]);
        } catch (Exception e) {
            return;
        }

        //Check if product column is empty. If it is, return.
        if (columns[2].isEmpty()) {
            return;
        }

        //Check if country column is empty. If it is, return.
        if (columns[0].isEmpty()) {
            return;
        }

        String product_code = String.valueOf(product_code_number);
        String country = columns[0];

//      int year = Integer.parseInt(columns[1].isEmpty() ? "0" : columns[1]);
        String product_description = columns[3].isEmpty() ? "" : columns[3];
//      float price = Float.parseFloat(columns[5].isEmpty() ? "0" : columns[5]);
//      float weight = Float.parseFloat(columns[6].isEmpty() ? "0" : columns[6]);
//      String un = columns[7];
//      float quantity = Float.parseFloat(columns[8].isEmpty() ? "0" : columns[8]);

        // Return if it's not Brazil
        if (!country.equals("Brazil")) {
            return;
        }

        // Write to context
        context.write(
                new IntStringCompositeKeyWritable(product_code_number, product_description),
                new IntWritable(1)
        );

    }
}