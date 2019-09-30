package transactionsmapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class AvgPricePerWPerCommPerYOnBrazilMapper extends Mapper<Object, Text, IntStringCompositeKeyWritable, IntFloatWritable> {

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

        //Check if price column is empty. If it is, return.
        if (columns[5].isEmpty()) {
            return;
        }

        //Check if weight column is empty. If it is, return.
        if (columns[6].isEmpty()) {
            return;
        }

        String country = columns[0];
        int year = Integer.parseInt(columns[1]);
        String product_code = String.valueOf(product_code_number);
        float price = Float.parseFloat(columns[5]);
        float weight = Float.parseFloat(columns[6]);


        //Check if weight is 0. If it is, we return. Needed, else we 'll see infinite values...
        if (weight == 0) {
            return;
        }

        // if country isn't Brazil, return.
        if (!country.equals("Brazil")) {
            return;
        }

        context.write(new IntStringCompositeKeyWritable(year, product_code),
                new IntFloatWritable(1, price/weight));
    }

}