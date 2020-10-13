package com.btw.parser.util;

import com.fate.decompress.DecompressFactory;
import com.fate.decompress.ReaderHandler;
import com.fate.decompress.UnzipFile;
import com.fate.file.parse.DBSteerableConfig;
import com.fate.file.parse.processor.FileProcessor;
import com.fate.file.parse.processor.LineProcessor;
import com.fate.file.transfer.FileSelector;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

//SteerableParserIntegrator 不使用logMapper不会生成日志和报警
public class SteerableParserIntegratorTest {

    @Test
    public void testUnzipNoFile() throws Exception {

    }

    /**
     * 不带日志的无文件生成解压入库
     * DecompressFactory 提供一些简单场景封装
     * @param handler
     * @param delete
     * @throws Exception
     */
    public void unzipNoFile(ReaderHandler handler, boolean delete) throws Exception {
        //参数
        String saveDir = "";
        FileSelector fileSelector = null;
        DBSteerableConfig config = null;
        String fileType = "";

        File[] files = new File(saveDir).listFiles();
        if (files != null) {
            Arrays.sort(files);
            for (File file : files) {
                String name = file.getName();
                String order = fileSelector.getOrder(name);
                if (fileSelector.acceptFile(name) && fileSelector.acceptOrder(order)) {
                    //下两行核心，单文件
                    DecompressFactory factory = new DecompressFactory(new UnzipFile(), handler);
                    factory.decompressNoFile(file);

                    config.updateOrder(fileType, order);
                    if (delete) {
                        file.delete();
                    }
                }
            }
        }
    }

    /**
     * 不带日志的文件解析入库
     * FileProcessor.getInstance() 相比日志代理，多一些简单场景封装
     * @param dir
     * @param lineProcessor
     * @param delete
     * @param <T>
     * @throws Exception
     */
    public <T> void parse(String dir, LineProcessor<T> lineProcessor, boolean delete) throws Exception{
        //参数
        FileSelector fileSelector = null;
        DBSteerableConfig config = null;
        String fileType = "";

        File[] files = new File(dir).listFiles();
        assert files != null;
        for (File file : files) {
            Arrays.sort(files);
            String name = file.getName();
            String order = fileSelector.getOrder(name);
            if (fileSelector.acceptFile(name) && fileSelector.acceptOrder(order)) {
                //该行核心，单文件
                FileProcessor.getInstance().process(file, lineProcessor);

                config.updateOrder(fileType, order);
                if (delete) {
                    file.delete();
                }
            }
        }
    }
}