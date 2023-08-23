package Utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HTTP;
import za.co.absa.spline.harvester.dispatcher.AbstractJsonLineageDispatcher;
import java.io.BufferedReader;
import java.io.InputStreamReader;
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
            JSONObject operations = JSONObject.parseObject(replaceDate).getJSONObject("operations");
            JSONObject write = operations.getJSONObject("write");
            JSONObject params = write.getJSONObject("params");

            //目标数据库
            String targetDatabase = params.getString("hoodie.datasource.hive_sync.database");
            //目标表
            String targetTablename = params.getString("hoodie.datasource.hive_sync.table");

            String json= "{downstreamUrn: \"urn:li:dataset:(urn:li:dataPlatform:hive,"+targetDatabase+"."+targetTablename+",PROD)\",";
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
                String upstreamUrn =   "upstreamUrn :  \"urn:li:dataset:(urn:li:dataPlatform:hive,"+s+",PROD)\"}";
                String  jsonParam =   json+upstreamUrn;
                jsonParamList.add(jsonParam);
            }

            StringBuilder sb = new StringBuilder();
            for(int i = 0; i < jsonParamList.size()-1; i++) {
                sb.append(jsonParamList.get(i)).append(",");
            }
            if(!jsonParamList.isEmpty()) {
                sb.append(jsonParamList.get(jsonParamList.size()-1));
            }

            String spline = "{\"query\":\"mutation updateLineage { updateLineage( input:{ edgesToAdd : [" + sb + "],edgesToRemove: []})}\",\"variables\":{}}";

          handleHttp(spline,"http://172.18.x.xx:9002/api/graphql");

        }

        }
    public  void handleHttp(String jsonParam, String url) {
        BufferedReader in = null;
        try {
            HttpClient client = HttpClients.createDefault();
            HttpPost request = new HttpPost(url);

            request.addHeader(HTTP.CONTENT_TYPE, "application/json");
            request.addHeader("Authorization","Bearer xx");

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


