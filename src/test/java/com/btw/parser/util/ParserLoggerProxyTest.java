package com.btw.parser.util;

import com.btw.parser.mapper.ParserLogMapper;
import com.fate.decompress.DecompressFile;
import com.fate.decompress.NormalReaderHandler;
import com.fate.decompress.UnzipFile;
import com.fate.log.ParserLoggerProxy;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.File;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:applicationContext.xml")
public class ParserLoggerProxyTest {

    private ParserLogMapper logMapper;

    @Autowired
    public void setLogMapper(ParserLogMapper logMapper) {
        this.logMapper = logMapper;
    }

    @Test
    public void testGetTarget() throws Exception {
        DecompressFile fileProxy = new ParserLoggerProxy(logMapper, "test", "text.zip", new UnzipFile()).getTarget();
        fileProxy.doWith(new File("C:\\Users\\T440\\Desktop\\beans\\TAX.2007051300.zip"), "C:\\Users\\T440\\Desktop\\beans\\unzip.txt", new NormalReaderHandler());
    }
}