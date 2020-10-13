package com.btw.parser.util;

import com.btw.parser.mapper.ParserLogMapper;
import com.fate.log.IParserSend;

import java.util.List;

/**
 * Created by ydc on 2020/8/26.
 */
public class WeChatFare implements IParserSend{

    //ACCESS_TOKEN
    public static String ACCESS_TOKEN_STATIC = "";
    //ACCESS_TOKEN
    private final static String AGENTID = "1000009";
    // 企业号 ID
    private final static String CORPID = "ww3116d601b9d1b3fd";
    // 企业号 秘钥
    private final static String CORPSECRET = "9LQYD-iqJdU249GmC_UNG4ZUOrnF0XovQjgaZUyW79c";
    //private final static String CORPSECRET ="Ps71BRqnJr99grtGf8dv_pyGJo0lUrYm6o7QrWNlRcGbEBscCa83-ZVN5vr7vT2X";
    // 获取访问权限码URL
    private final static String ACCESS_TOKEN_URL = "https://qyapi.weixin.qq.com/cgi-bin/gettoken";
    // 创建会话请求URL
    private final static String CREATE_SESSION_URL = "https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token=";

    private ParserLogMapper logMapper;

    public WeChatFare(ParserLogMapper logMapper) {
        this.logMapper = logMapper;
    }

    @Override
    public void sendMessage(String fileType, String message) {
        List<String> user = logMapper.queryWXWarn(fileType);
        String[] u = user.toArray(new String[user.size()]);
        sendText(getSendUsers(u), "", "", message, WeChatFare.ACCESS_TOKEN_STATIC);
    }

    public static String getSendUsers(String... user){
        StringBuilder u = new StringBuilder();
        for(int i = 0,l = user.length; i < l; i++){
            if(i == l - 1){
                u.append(user[i]);
            }else {
                u.append(user[i]).append("|");
            }
        }
        return u.toString();
    }

//    // 获取 HttpClient 公用方法
//    public static HttpClient getClient(){
//        HttpClient client = new HttpClient();
//        //连接超时
//        client.getHttpConnectionManager().getParams().setConnectionTimeout(10000);
//        //请求超时
//        client.getHttpConnectionManager().getParams().setSoTimeout(10000);
////        client.getHostConfiguration().setProxy("172.31.1.246", 8080);
//        return client;
//    }
//
//    //获取微信token
//    public static String getAccessTokenGet(){
//        HttpClient client = getClient();
//        String result = "";
//        //创建get方法的实例
//        GetMethod getHttp = new GetMethod(ACCESS_TOKEN_URL+"?corpid="+CORPID+"&corpsecret="+CORPSECRET);
//        //使用系统提供的默认恢复策略
//        //getMethod.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler());
//        try {
//            int statusCode = client.executeMethod(getHttp);
//            result = new String(getHttp.getResponseBodyAsString().getBytes("gbk"));
//            // 将数据转换成json
//            System.out.println(result);
//            JSONObject jasonObject = JSONObject.fromObject(result);
//            result = (String) jasonObject.get("access_token");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        getHttp.releaseConnection();
//        return result;
//    }
//
    /**
     * 企业接口向下属关注用户发送微信消息(实现方式二)
     * @param touser  为企业通讯录用户名（英文，例如 zhubeibei）
     *            成员ID列表（消息接收者，多个接收者用‘|’分隔，最多支持1000个）。特殊情况：指定为@all，则向关注该企业应用的全部成员发送
     * @param toparty
     *            部门ID列表，多个接收者用‘|’分隔，最多支持100个。当touser为@all时忽略本参数
     * @param totag
     *            标签ID列表，多个接收者用‘|’分隔。当touser为@all时忽略本参数
     * @param content
     *            消息内容
     * @return
     */
    public static String sendText(String touser, String toparty, String totag, String content, String accessToken) {
//        HttpClient client = getClient();
//
//        client.getHttpConnectionManager().getParams().setConnectionTimeout(60000);
//        client.getHttpConnectionManager().getParams().setSoTimeout(60000);

        String result = "";
        String ACCESS_TOKEN = accessToken;
//        if(StringUtils.isBlank(ACCESS_TOKEN)){
//            ACCESS_TOKEN = getAccessTokenGet();
//        }
//        PostMethod postHttp = new PostMethod(CREATE_SESSION_URL + ACCESS_TOKEN);
//        postHttp.setRequestHeader("Content-Type","application/json;charset=UTF-8");
//        // 设置策略，防止报cookie错误
//        DefaultHttpParams.getDefaultParams().setParameter("http.protocol.cookie-policy", CookiePolicy.BROWSER_COMPATIBILITY);
//        // 封装发送消息请求json
//        StringBuffer sb = new StringBuffer();
//        String json = sb.toString();
//        json=STextMsg(touser,toparty,totag,AGENTID,content);
//        System.out.println(json);
//        try {
//            // 给post设置参数
//            RequestEntity requestEntity = new StringRequestEntity(json,"text/xml","UTF-8");
//
//            postHttp.setRequestEntity(requestEntity);
//            client.executeMethod(postHttp);
//            result = new String(postHttp.getResponseBodyAsString().getBytes("UTF-8"));
//            System.out.println(result);
//            //返回 {"errcode":0,"errmsg":"ok"}
//            if(result!=null&&result.contains("ok")){
//                return result;
//            }else{
//                return "";
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//
//            // 给post设置参数
//            RequestEntity requestEntity;
//            try {
//                requestEntity = new StringRequestEntity(json,"text/xml","UTF-8");
//                postHttp.setRequestEntity(requestEntity);
//                client.executeMethod(postHttp);
//            } catch (Exception e1) {
//                e1.printStackTrace();
//            }
//
//        }
        System.out.println("WeiChat:" + content);
        return result;
    }

    /**
     * text消息
     *
     * @param touser  UserID列表（消息接收者，多个接收者用‘|’分隔）。特殊情况：指定为@all，则向关注该企业应用的全部成员发送————"touser": "UserID1|UserID2|UserID3"
     * @param toparty PartyID列表，多个接受者用‘|’分隔。当touser为@all时忽略本参数————"toparty": " PartyID1 | PartyID2 "
     * @param totag   TagID列表，多个接受者用‘|’分隔。当touser为@all时忽略本参数————"totag": " TagID1 | TagID2 "
     * @param agentid 企业应用的id，整型。可在应用的设置页面查看
     * @param content 消息内容
     */
    public static String STextMsg(String touser, String toparty, String totag, String agentid, String content) {
        String PostData = "{\"touser\": \"%s\",\"toparty\": \"%s\",\"totag\": \"%s\",\"msgtype\": \"text\",\"agentid\": \"%s\",\"text\": {\"content\": \"%s\"},\"safe\":\"0\"}";
        return String.format(PostData, touser, toparty, totag, agentid, content);
    }
}
