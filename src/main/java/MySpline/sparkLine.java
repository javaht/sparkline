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


        Dataset<Row> ds = session.sql("  with d1 as (select b.co_id, " +
                "                   co_name, " +
                "                   change_date, " +
                "                   annotation, " +
                "                   change_after, " +
                "                   create_date, " +
                "                   change_before, " +
                "                   change_project " +
                "            from (select co_id, " +
                "                         co_name, " +
                "                         change_date, " +
                "                         annotation, " +
                "                         change_after, " +
                "                         create_date, " +
                "                         change_before, " +
                "                         regexp_replace(change_project,'<br>|br>|r>|>','') as change_project, " +
                "                         row_number() over (partition by co_name,change_date,change_project,change_before,change_after order by length(co_id) asc,partitionpath desc) as row_num " +
                "                  from (select if(co_id is not null and !array_contains(array('', '-', 'None'), co_id), co_id, " +
                "                                  null) as co_id, " +
                "                               if(co_name is not null and !array_contains(array('', '-', 'None'), co_name), translate( " +
                "                                       if(regexp (co_name, '\\\\w'), " +
                "                                          regexp_replace(regexp_replace(trim(co_name), \" （\", \"（\"), '） ', " +
                "                                                         '）'), " +
                "                                          regexp_replace(co_name, \" |\u001F|\\r|\\t|\\n|[\\\\s]+|[\\u3000]+\", \"\"))," +
                "                                       '()', " +
                "                                       '（）'), " +
                "                                  null) as co_name, " +
                "                               change_date, " +
                "                               if(annotation is not null and !array_contains(array('', '-', 'None'), annotation), " +
                "                                  regexp_replace(annotation, '<[^>]+>', ''), " +
                "                                  null) as annotation, " +
                "                               if(change_after is not null and !array_contains(array('', '-', 'None'), change_after), " +
                "                                  regexp_replace(regexp_replace(change_after, '<[^>]+>', ''), '\\r|\\n|\\t', '')," +
                "                                  null) as change_after," +
                "                               create_date, " +
                "                               if(change_before is not null and " +
                "                                  !array_contains(array('', '-', 'None'), change_before), " +
                "                                  regexp_replace(regexp_replace(change_before, '<[^>]+>', ''), '\\r|\\n|\\t', '')," +
                "                                  null) as change_before, " +
                "                               if(change_project is not null and " +
                "                                  !array_contains(array('', '-', 'None'), change_project), " +
                "                                  change_project, " +
                "                                  null) as change_project, " +
                "                               partitionpath " +
                "                        from hudi_ods.ods_company_change_info) a) b " +
                "            where b.row_num = 1), " +
                "     d2 as (select co_name, co_code from hudi_dwd.dwd_company_code), " +
                "     d3 as (select tyc_id, co_code from hudi_dwd.dwd_company_code) " +
                "select `if`(d2.co_code is not null, d2.co_code, " +
                "            `if`(d3.co_code is not null, d3.co_code, null)) as co_code,        " +
                "       d1.co_id                                             as co_id,          " +
                "       d1.co_name                                           as co_name,        " +
                "       d1.change_date                                       as change_date,    " +
                "       d1.annotation                                        as annotation,    " +
                "       d1.change_after                                      as change_after,   " +
                "       d1.create_date                                       as create_date,   " +
                "       d1.change_before                                     as change_before, " +
                "       d1.change_project                                    as change_project  from d1  left join d2 on d1.co_name = d2.co_name left join d3 on d1.co_id = d3.tyc_id  ");


        Dataset endDs = SparkUtilByJava.odsProcess(ds, session);

        String path = "hdfs:///datas/hudi_datas/hive/test/";
        String database = "test";
        String table = "targatest";

         SparkUtilByJava.save(endDs,path,database,table);


        session.close();


    }
}
