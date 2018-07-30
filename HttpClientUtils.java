package cn.person.httpclient;

import com.sun.org.apache.regexp.internal.REUtil;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.*;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class HttpClientUtils {

    public static void main(String[] args) {
        HttpClientUtils utils = new HttpClientUtils();
        try {
            HttpResult result = utils.doGet("http://manager.market.com/rest/item/interface/41");
            System.out.println(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 发起无参数的get请求
     * @param url
     * @return
     * @throws Exception
     */
    public HttpResult doGet(String url) throws Exception {
        return doGet(url, null);
    }

    /**
     * 发起带参数的get请求
     * @param url
     * @param map
     * @return
     * @throws Exception
     */
    public HttpResult doGet(String url, Map<String,Object> map) throws Exception {
        HttpResult result = new HttpResult();
        // 创建Httpclient对象
        CloseableHttpClient httpclient = HttpClients.createDefault();

        // 定义请求的参数
        //URI uri = new URIBuilder("http://www.baidu.com/s").setParameter("wd", "java").build();
        //设置参数
        URIBuilder uriBuilder = new URIBuilder(url);
        if (map != null) {
            for (String key : map.keySet()) {
                uriBuilder.setParameter(key, map.get(key).toString());
            }
        }

        // 创建http GET请求
        HttpGet httpGet = new HttpGet(uriBuilder.build());

        CloseableHttpResponse response = null;
        try {
            // 执行请求
            response = httpclient.execute(httpGet);
            // 判断返回状态是否为200
            int statusCode = response.getStatusLine().getStatusCode();
            result.setCode(statusCode);

            if (statusCode == 200) {
                String content = EntityUtils.toString(response.getEntity(), "UTF-8");
                result.setBody(content);
            }
        } finally {
            if (response != null) {
                response.close();
            }
            httpclient.close();
        }
        return result;
    }

    /**
     * 发起带参post请求
     * @param url
     * @param map
     * @return
     * @throws Exception
     */
    public HttpResult doPost(String url, Map<String,Object> map) throws Exception{
        HttpResult result = new HttpResult();
        // 创建Httpclient对象
        CloseableHttpClient httpclient = HttpClients.createDefault();

        // 创建http POST请求
        HttpPost httpPost = new HttpPost(url);

        // 设置参数
        List<NameValuePair> parameters = new ArrayList<NameValuePair>(0);
        for (String key : map.keySet()) {
            parameters.add(new BasicNameValuePair(key, map.get(key).toString()));
        }
        // 构造一个form表单式的实体
        UrlEncodedFormEntity formEntity = new UrlEncodedFormEntity(parameters,"UTF-8");
        // 将请求实体设置到httpPost对象中
        httpPost.setEntity(formEntity);

        CloseableHttpResponse response = null;
        try {
            // 执行请求
            response = httpclient.execute(httpPost);
            // 判断返回状态是否为200
            int statusCode = response.getStatusLine().getStatusCode();
            result.setCode(statusCode);

            if (statusCode == 201) {
                String content = EntityUtils.toString(response.getEntity(), "UTF-8");
                result.setBody(content);
            }
        } finally {
            if (response != null) {
                response.close();
            }
            httpclient.close();
        }
        return result;
    }


    /**
     * 发起带参put请求
     * @param url
     * @param map
     * @return
     * @throws Exception
     */
    public HttpResult doPut(String url, Map<String,Object> map) throws Exception{
        HttpResult result = new HttpResult();
        // 创建Httpclient对象
        CloseableHttpClient httpclient = HttpClients.createDefault();

        // 创建http POST请求
        HttpPut httpPut = new HttpPut(url);

        // 设置参数
        List<NameValuePair> parameters = new ArrayList<NameValuePair>(0);
        for (String key : map.keySet()) {
            parameters.add(new BasicNameValuePair(key, map.get(key).toString()));
        }
        // 构造一个form表单式的实体
        UrlEncodedFormEntity formEntity = new UrlEncodedFormEntity(parameters,"UTF-8");
        // 将请求实体设置到httpPut对象中
        httpPut.setEntity(formEntity);

        CloseableHttpResponse response = null;
        try {
            // 执行请求
            response = httpclient.execute(httpPut);
            // 判断返回状态是否为200
            int statusCode = response.getStatusLine().getStatusCode();
            result.setCode(statusCode);

            if (statusCode == 204) {
                String content = EntityUtils.toString(response.getEntity(), "UTF-8");
                result.setBody(content);
            }
        } finally {
            if (response != null) {
                response.close();
            }
            httpclient.close();
        }
        return result;
    }

    /**
     * 发起带参数的delete请求
     * @param url
     * @param map
     * @return
     * @throws Exception
     */
    public HttpResult doDelete(String url, Map<String,Object> map) throws Exception {
        HttpResult result = new HttpResult();
        // 创建Httpclient对象
        CloseableHttpClient httpclient = HttpClients.createDefault();

        // 定义请求的参数
        //URI uri = new URIBuilder("http://www.baidu.com/s").setParameter("wd", "java").build();
        //设置参数
        URIBuilder uriBuilder = new URIBuilder(url);
        if (map != null) {
            for (String key : map.keySet()) {
                uriBuilder.setParameter(key, map.get(key).toString());
            }
        }

        // 创建http GET请求
        HttpDelete httpDelete = new HttpDelete(uriBuilder.build());

        CloseableHttpResponse response = null;
        try {
            // 执行请求
            response = httpclient.execute(httpDelete);
            // 判断返回状态是否为200
            int statusCode = response.getStatusLine().getStatusCode();
            result.setCode(statusCode);

            if (statusCode == 204) {
                String content = EntityUtils.toString(response.getEntity(), "UTF-8");
                result.setBody(content);
            }
        } finally {
            if (response != null) {
                response.close();
            }
            httpclient.close();
        }
        return result;
    }
}
