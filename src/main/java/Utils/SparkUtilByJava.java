package Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.sync.common.HoodieSyncConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.apache.hudi.common.table.HoodieTableConfig.*;

public class SparkUtilByJava {

    public static SparkSession createSparkSession() throws IOException {
        initKerberos();
       String master= "local[*]";
        System.setProperty("HADOOP_USER_NAME", "root");
        return SparkSession.builder()
                .appName("heihei")
                .master(master)
                .config("spark.sql.catalogImplementation", "hive")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
                .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
                .config("spark.jars.package", "org.apache.hudi:hudi-spark3-bundle_2.12:0.12.2")
                .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
                .getOrCreate();
    }

    public static Dataset odsProcess(Dataset ds, SparkSession session){

        // 获取第一行数据
        ds.first();
        Date date = new Date();
        SimpleDateFormat partitionpath_format = new SimpleDateFormat("yyyy");
        SimpleDateFormat data_createtime_format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String partitionpath_str = partitionpath_format.format(date);
        String data_createtime_str = data_createtime_format.format(date);

        Dataset df = ds.withColumn("uuid_mid", functions.row_number().over(Window.orderBy("co_id")))
                .withColumn("uuid", functions.col("uuid_mid"))
                .withColumn("data_createtime",functions.lit(data_createtime_str))
                .withColumn("data_updatetime", functions.lit(data_createtime_str))   // 数据最后更新时间
                .withColumn("status", functions.lit(1))
                .withColumn("ts", functions.lit(date.getTime())) // 添加timestamp列，作为Hudi表记录数据与合并时字段，使用发车时间
                .withColumn("partitionpath",functions.lit(partitionpath_str)) // hudi 指定的分区字段
                .drop("uuid_mid");

        return df;
    }


     public static void save(Dataset ds,String path,String dataBase,String tablename){

         ds.write().format("hudi")
                 .option(String.valueOf(DataSourceWriteOptions.TABLE_TYPE()), DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL())
                 .option(HoodieIndexConfig.INDEX_TYPE.key(), HoodieIndex.IndexType.GLOBAL_BLOOM.name()) //使用的过滤器类型是全局布隆过滤器
                 .option("hoodie.datasource.write.precombine.field", "ts") //当数据主键相同时，对比的字段,保存该字段大的数据
                 .option("hoodie.datasource.write.recordkey.field", "co_id") //设置主键列名称
                 .option("hoodie.datasource.write.partitionpath.field", "partitionpath")     //指定分区列
                 //并行度设置
                 .option("hoodie.insert.shuffle.parallelism", "2")
                 .option("hoodie.upsert.shuffle.parallelism", "2")
                 .option("hoodie.table.name", tablename)
                 //指定HiveServer2 连接url
                 .option(HiveSyncConfig.HIVE_URL.key(),"jdbc:hive2://172.18.x.x:10000/;principal=hive/xx@xxx.COM")
                 .option(HiveSyncConfig.METASTORE_URIS.key(),"thrift://172.18.x.5xx:9083")
                 //指定Hive 对应的库名
                 .option(HoodieSyncConfig.META_SYNC_DATABASE_NAME.key(),dataBase)
                 //指定Hive映射的表名称
                 .option(HoodieSyncConfig.META_SYNC_TABLE_NAME.key(),tablename)
                 //Hive表映射对的分区字段
                 .option(HoodieSyncConfig.META_SYNC_PARTITION_FIELDS.key(),"date_strs")
                 //当设置为true时，注册/同步表到Apache Hive metastore,默认是false，这里就是自动创建表
                 .option(HiveSyncConfig.HIVE_SYNC_ENABLED.key(),"true")
                 //如果分区格式不是yyyy/mm/dd ，需要指定解析类将分区列解析到Hive中
                 .option(HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS.key(),"org.apache.hudi.hive.MultiPartKeysValueExtractor")
                 .mode(SaveMode.Overwrite)
                 .save(path);
     }


    public static void initKerberos() throws IOException {
        String path = "D:\\soft\\IdeaWorkSpace\\dataCompare\\src\\main\\resources\\";
        Configuration conf = new Configuration();
        System.setProperty("java.security.krb5.conf", path + "krb5.conf");
        conf.addResource(new Path(path + "hdfs-site.xml"));
        conf.addResource(new Path(path + "hive-site.xml"));
        conf.set("hadoop.security.authentication", "Kerberos");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("dfs.client.use.datanode.hostname", "true");
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab("hive/datasophon01@HADOOP.COM", path + "hive.service.keytab");
        System.out.println("login user: "+UserGroupInformation.getLoginUser());
    }




    public static void main(String[] args) throws IOException {


    }



}
