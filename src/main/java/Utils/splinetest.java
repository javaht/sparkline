package Utils;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HTTP;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

public class splinetest {
    public static void main(String[] args) {

        String jsonParam = " {\"query\": \"mutation updateLineage { updateLineage( input:{ edgesToAdd : [{ downstreamUrn: \\\"urn:li:dataset:(urn:li:dataPlatform:hive,hudi_dwd.dwd_company_weibo,PROD)\\\", upstreamUrn : \\\"urn:li:dataset:(urn:li:dataPlatform:hive,hudi_ods.ods_company_weibo,PROD)\\\"}], edgesToRemove :[] })}\",\"variables\": {}} ";


        handleHttp(jsonParam,"http://172.18.x.xx:9002/api/graphql");

    }

    public static void handleHttp(String jsonParam, String url) {
        BufferedReader in = null;
        try {
            HttpClient client = HttpClients.createDefault();
            HttpPost request = new HttpPost(url);

            request.addHeader(HTTP.CONTENT_TYPE, "application/json");
           request.addHeader("Authorization","Bearer xxx");

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
