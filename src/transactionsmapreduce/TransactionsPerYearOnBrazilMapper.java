package transactionsmapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TransactionsPerYearOnBrazilMapper extends Mapper<Object, Text, IntStringCompositeKeyWritable, IntWritable> {

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

        //Check if year column is empty. If it is, return.
        if (columns[1].isEmpty()) {
            return;
        }

        int year = Integer.parseInt(columns[1]);

        String country = columns[0];

        String product_description = columns[3].isEmpty() ? "" : columns[3];

        // if country isn't Brazil, return.
        if (!country.equals("Brazil")) {
            return;
        }

        // if year isn't 2016, return.
        if (year != 2016) {
            return;
        }

        // Write to context
        context.write(
                new IntStringCompositeKeyWritable(product_code_number, product_description),
                new IntWritable(1)
        );


    }

}