package transactionsmapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class AvgWeightPerCommodityPerYearMapper extends Mapper<Object, Text, IntStringCompositeKeyWritable, IntFloatWritable> {

    public void map(Object key, Text value, Context context) throws IOException,
            InterruptedException {

        //country_or_area;year;comm_code;commodity;flow;trade_usd;weight_kg;;quantity;category

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

        //Check if country column is empty. If it is, return.
        if (columns[0].isEmpty()) {
            return;
        }

        //Check if product column is empty. If it is, return.
        if (columns[2].isEmpty()) {
            return;
        }


        //Check if weight column is empty. If it is, return.
        if (columns[6].isEmpty()) {
            return;
        }

        int year = Integer.parseInt(columns[1]);
        String product_code = columns[2];
        float weight = Float.parseFloat(columns[6]);

        context.write(new IntStringCompositeKeyWritable(year, product_code),
                new IntFloatWritable(1, weight));
    }

}