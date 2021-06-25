package com.sparkbyexamples.java;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.InputStream;
import java.util.Properties;

public class GetDailyProductRevenue {

    public static void main(String... arg){
     try(InputStream in = GetDailyProductRevenue.class.getResourceAsStream("/application.properties")){
         Properties properties = new Properties();
         properties.load(in);
         String executionMode=properties.getProperty(arg[0]+".execution.mode");
         String inputDir = properties.getProperty(arg[0]+".input.base.dir");
         String outputDir = properties.getProperty(arg[0]+".output.base.dir");

         SparkSession sparkSession = SparkSession.builder()
                                    .appName("Get Daily Product Revenue")
                 .master(executionMode)
                 .getOrCreate();
         sparkSession.sparkContext().setLogLevel("INFO");
         sparkSession.conf().set("spark.sql.shuffle.partitions", "2");
         Dataset<Row> ordersDataSet =  sparkSession.read().
                 schema("order_id INT," +
                         "order_date STRING," +
                         "order_customer_id INT," +
                         "order_status STRING")
                        .csv(inputDir+"/orders");

         Dataset<Row> orderItemsDataSet = sparkSession.read()
                 .option("inferSchema", "true")
                 .schema("\n" +
                         "         order_item_id INT,\n" +
                         "         order_item_order_id INT,\n" +
                         "         order_item_product_id INT,\n" +
                         "         order_item_quantity INT,\n" +
                         "         order_item_subtotal FLOAT,\n" +
                         "         order_item_product_price FLOAT")
                 .csv(inputDir+"/order_items");

        Dataset<Row> completedOrders =   ordersDataSet
                 .where(" order_status in ('CLOSED', 'COMPLETE')");
        completedOrders.groupBy("order_date").count().write().mode("overwrite").
                 json(outputDir + "/completed_orders");


     }catch (Exception e){
         System.out.println(e.toString());

     }

    }
}
