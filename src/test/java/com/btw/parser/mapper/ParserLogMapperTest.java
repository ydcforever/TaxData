package com.btw.parser.mapper;

import com.fate.log.ParserLogger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext.xml")
public class ParserLogMapperTest {

    @Autowired
    private ParserLogMapper logMapper;

    @Test
    public void testInsertLog() throws Exception {
        ParserLogger logger = new ParserLogger("TEST","TEST", logMapper);
        logger.start();
        logger.setStatus("Y");
        logger.end();
    }

    @Test
    public void testQueryWXWarn() throws Exception {
        List<String> user = logMapper.queryWXWarn("TTBS");
        for (String s : user) {
            System.out.println(s);
        }
    }
}