package com.btw.parser.service;

import junit.framework.TestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext.xml")
public class IcerParserTest extends TestCase {

    @Autowired
    IcerParser icerParser;

    @Test
    public void testDoTask() throws Exception {
        icerParser.doTask();
    }

    @Test
    public void testDoBatch() throws Exception {
        icerParser.doBatch();
    }
}