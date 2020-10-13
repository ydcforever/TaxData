package com.btw.parser.util;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class WeChatFareTest {

    @Test
    public void testSendText() throws Exception {
            //getAccessTokenPost();
            //发送多人用|分隔符，例如TianYao|YangDongCheng，如发送部门所有人用@all，使用前需确认
//        WeChatFare.sendText("TianYao", "", "", " testing2。", WeChatFare.ACCESS_TOKEN_STATIC);
        List<String> a = new ArrayList<>();
        a.add("1");
        a.add("2");
        String[] f = a.toArray(new String[a.size()]);
        System.out.println(f[1]);
    }

    @Test
    public void testGetSendUsers() throws Exception {
        System.out.println(WeChatFare.getSendUsers("ty", "ydc"));
    }
}