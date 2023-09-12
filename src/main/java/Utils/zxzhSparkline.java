package Utils;

import Entitys.targetTable;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.data.template.GetMode;
import com.linkedin.schema.*;
import datahub.client.Callback;
import datahub.client.MetadataWriteResponse;
import datahub.client.rest.RestEmitter;
import datahub.event.MetadataChangeProposalWrapper;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HTTP;
import za.co.absa.spline.harvester.dispatcher.AbstractJsonLineageDispatcher;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.*;

public class zxzhSparkline extends AbstractJsonLineageDispatcher {

    @Override
    public String name() {
        String name  = "zxjson";
        return name;
    }

    @Override
    public void send(String data) {
        if (data.startsWith("ExecutionPlan")) {
            String replaceDate = StringUtils.replace(data, "ExecutionPlan (apiVersion: 1.2):", "");

            //这里拼接的血缘
            StringBuilder sb = makeLine(replaceDate);
            //先删除原来的数据血缘
            String  splineRemove =  "{\"query\": \"mutation updateLineage { updateLineage( input:{ edgesToAdd : [],edgesToRemove: [" + sb + "]})}\",\"variables\":{}}";
            //增加最新的数据血缘
            String splineAdd = "{\"query\": \"mutation updateLineage { updateLineage( input:{ edgesToAdd : [" + sb + "],edgesToRemove: []})}\",\"variables\":{}}";

           // handleHttp(splineRemove,"http://172.18.1.54:9002/api/graphql");
         //   handleHttp(splineAdd,"http://172.18.1.54:9002/api/graphql");

        }


    }

 /*
 *
 * 这个解析列级别的数据血缘
 *
 * */
    public StringBuilder makeLine(String replaceDate){
        String downstreamUrn = "";
        JSONObject operations = JSONObject.parseObject(replaceDate).getJSONObject("operations");
        JSONObject write = operations.getJSONObject("write");
        String type = write.getJSONObject("extra").getString("destinationType");

        if("hudi".equals(type)){
            JSONObject params = write.getJSONObject("params");
            //目标数据库
            String targetDatabase = params.getString("hoodie.datasource.hive_sync.database");
            //目标表
            String targetTablename = params.getString("hoodie.datasource.hive_sync.table");

            downstreamUrn= "{downstreamUrn: \\\"urn:li:dataset:(urn:li:dataPlatform:hive,"+targetDatabase+"."+targetTablename+",PROD)\\\",";
        }

        JSONArray readsArray = operations.getJSONArray("reads"); //这里开始获取数据来源
        Set<String> sourset =  new LinkedHashSet<String>();//定义一个不允许重复的集合
        for (int i = 0; i < readsArray.size(); i++) {
            JSONObject readObj = readsArray.getJSONObject(i);
            String sourceTable = readObj.getJSONObject("params").getJSONObject("table").getJSONObject("identifier").getString("table");
            String sourceDatabase = readObj.getJSONObject("params").getJSONObject("table").getJSONObject("identifier").getString("database");
            sourset.add(sourceDatabase+"."+sourceTable);
        }

        List jsonParamList = new ArrayList<String>();
        for(String s: sourset){
            String upstreamUrn =   "upstreamUrn :  \\\"urn:li:dataset:(urn:li:dataPlatform:hive,"+s+",PROD)\\\"}";
            String  jsonParam =   downstreamUrn+upstreamUrn;
            jsonParamList.add(jsonParam);
        }

        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < jsonParamList.size()-1; i++) {
            sb.append(jsonParamList.get(i)).append(",");
        }
        if(!jsonParamList.isEmpty()) {
            sb.append(jsonParamList.get(jsonParamList.size()-1));
        }

        return sb;

    }



    public  void  sendTargetTable(List<targetTable> list) throws IOException, URISyntaxException {

       RestEmitter emitter = RestEmitter.create(b -> b.server("http://172.18.1x.54x:x").timeoutSec(100).token("x.x.x"));


       SchemaMetadata metadata = new SchemaMetadata();
       DatasetUrn.createFromString("urn:li:dataPlatform:hive");
       String schemaname = "thisSchemaname";
       String tablename = "tablename";
       metadata.setSchemaName(schemaname);
       SchemaMetadata.PlatformSchema  platformSchema = new       SchemaMetadata.PlatformSchema();
       OtherSchema schema = new OtherSchema();
       schema.setRawSchema("rawschema_hive");
       platformSchema.setOtherSchema(schema);
       metadata.setPlatformSchema(platformSchema);
       DataPlatformUrn urn = new DataPlatformUrn("DataPlatformUrn_hive");
       metadata.setPlatform(urn);
       metadata.setVersion(0);
       metadata.setHash("");
       SchemaFieldArray fieldArray = new SchemaFieldArray();

       for(targetTable table: list){
           SchemaField field = new SchemaField();
           field.setDescription("");
           field.setFieldPath(table.getTablename());
           field.setNativeDataType(table.getTabletype());
           SchemaFieldDataType dataType =  field.getType(GetMode.valueOf(field.getNativeDataType()));
           field.setType(dataType);
           fieldArray.add(field);
       }
       metadata.setFields(fieldArray);

       MetadataChangeProposalWrapper mcpw = MetadataChangeProposalWrapper.builder()
               .entityType("dataset")
               .entityUrn("urn:li:dataset:(urn:li:dataPlatform:hive,hudi_dwd.test,PROD)")
               .upsert()
               .aspect(metadata)
               .build();

       emitter.emit(mcpw, new Callback() {
           @Override
           public void onCompletion(MetadataWriteResponse response) {
               if (response.isSuccess()) {
                   System.out.println(String.format("Successfully emitted metadata event for %s", mcpw.getEntityUrn()));
               } else {
                   // Get the underlying http response
                   HttpResponse httpResponse = (HttpResponse) response.getUnderlyingResponse();
                   System.out.println(String.format("Failed to emit metadata event for %s, aspect: %s with status code: %d", mcpw.getEntityUrn(), mcpw.getAspectName(), httpResponse.getStatusLine().getStatusCode()));


               }
           }

           @Override
           public void onFailure(Throwable exception) {
               System.out.println(
                       String.format("Failed to emit metadata event for %s, aspect: %s due to %s", mcpw.getEntityUrn(),
                               mcpw.getAspectName(), exception.getMessage()));
           }
       });



   }


    /*
    *
    * 这里解析目标表的结构和类型
    *
    * */
    public List makeTargetTable(String replaceDate){
        //这里先解析目标表的名字和类型。 因为解析出来的字段类型是加密的，所以我们要从columnTypes得到类型
        JSONArray columnNames = JSONObject.parseObject(replaceDate).getJSONArray("attributes");
        JSONArray columnTypes =        JSONObject.parseObject(replaceDate).getJSONObject("extraInfo").getJSONArray("dataTypes");
        ArrayList<targetTable> targetTablesList = new ArrayList<>();
        //首先遍历有具体类型名字的
        for (int i = 0; i < columnTypes.size(); i++) {
            JSONObject targetTb = JSONObject.parseObject(columnTypes.get(i).toString());
            String targetTypeid = targetTb.getString("id");
            String targetTypeName  = targetTb.getString("name");//这个是列类型

            //再遍历有具体列名的
            for(int j = 0; j < columnNames.size(); j++) {
                JSONObject attribute = JSONObject.parseObject(columnNames.get(j).toString());
                if(StringUtils.isEmpty(attribute.getString("childRefs"))){
                    String name = attribute.getString("name");  //这个是列名
                    String dataType = attribute.getString("dataType");

                    if(targetTypeid.equals(dataType)){
                        //说明是同一个类型 我们把列名和列的类型放到list里
                        targetTablesList.add(targetTable.builder().tabletype(targetTypeName).tablename(name).build());
                    }

                }

            }

        }
        return targetTablesList;
    }




    public  void handleHttp(String jsonParam, String url) {
        BufferedReader in = null;
        try {
            HttpClient client = HttpClients.createDefault();
            HttpPost request = new HttpPost(url);

            //这里的token每组不一样 需要数据组长生成添加使用。
            String token = "";
            request.addHeader(HTTP.CONTENT_TYPE, "application/json");
            request.addHeader("Authorization","Bearer "+token);

            StringEntity s = new StringEntity(jsonParam, Charset.forName("UTF-8"));
            s.setContentEncoding("UTF-8");
            s.setContentType("application/json;charset=UTF-8");
            request.setEntity(s);

            HttpResponse response = client.execute(request);
            int code = response.getStatusLine().getStatusCode();

            in = new BufferedReader(new InputStreamReader(response.getEntity().getContent(), "utf-8"));
            StringBuilder sb = new StringBuilder();
            String line = "";
            String NL = System.getProperty("line.separator");
            while ((line = in.readLine()) != null) {
                sb.append(line).append(NL);
            }
            in.close();

            if (code == 200) {
                //logger.info("接口:{},请求成功:" + sb.toString(), url);
                System.out.println("接口请求成功:" + sb.toString()+"        "+ url);
            } else if (code == 500) {
                System.out.println("服务器错误:" + sb + ",url:" + url);
            } else {
                System.out.println("接口未知的情况,code=" + code + "," + sb + ",url:" + url);
            }
        } catch (Exception e) {
            System.out.println("接口调用出现异常……");
        }
    }

    }


