package MySpline;

import Utils.SparkUtilByJava;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import za.co.absa.spline.harvester.SparkLineageInitializer;

import java.io.IOException;

public class sparkLine {
    public static void main(String[] args) throws IOException {

        SparkSession session = SparkUtilByJava.createSparkSession();

        SparkLineageInitializer.enableLineageTracking(session);


        Dataset<Row> ds = session.sql("  zxxx ");


        Dataset endDs = SparkUtilByJava.odsProcess(ds, session);

        String path = "hdfs:///datas/hudi_datas/hive/test/";
        String database = "test";
        String table = "targatest";

         SparkUtilByJava.save(endDs,path,database,table);


        session.close();


    }
}
