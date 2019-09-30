package transactionsmapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class MaxPricePerWeightMapper extends Mapper<Object, Text, Text, IntFloatWritable> {

    public void map(Object key, Text value, Context context) throws IOException,
            InterruptedException {

        //country_or_area;year;comm_code;commodity;flow;trade_usd;weight_kg;;quantity;category

        String line = value.toString();

        String[] columns = line.split(";");

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

        //Check if price column is empty. If it is, return.
        if (columns[5].isEmpty()) {
            return;
        }

        //Check if weight column is empty. If it is, return.
        if (columns[6].isEmpty()) {
            return;
        }

        float price = Float.parseFloat(columns[5]);
        float weight = Float.parseFloat(columns[6]);

        //Check if weight is 0. If it is, we return. Needed, else we 'll see infinite values...
        if (weight == 0) {
            return;
        }

        context.write(new Text("MaxPricePerWeight"),
                new IntFloatWritable(product_code_number, price/weight));
    }

}