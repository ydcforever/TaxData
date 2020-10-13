package com.btw.parser.service;

import com.btw.parser.mapper.ParserLogMapper;
import com.btw.parser.util.AuxiliaryUtils;
import com.btw.parser.util.SteerableParserIntegrator;
import com.fate.decompress.NoFileReaderHandler;
import com.fate.file.parse.processor.LineProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

/**
 * Created by ydc on 2019/7/11.
 */
@Service
public class YQYRParser {

    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ParserLogMapper logMapper;

    @Autowired
    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void doTask() throws Exception {
        final SteerableParserIntegrator integrator = new SteerableParserIntegrator(jdbcTemplate, "YQYR").logMapper(logMapper);
        final SteerableParserIntegrator.Insert cfgHeader = integrator.new Insert("Y_HEADER");
        AuxiliaryUtils.addSource(cfgHeader);

        final SteerableParserIntegrator.Insert cfgS1 = integrator.new Insert("S1");
        AuxiliaryUtils.addRowXS1(cfgS1);

        final SteerableParserIntegrator.Insert cfgS2 = integrator.new Insert("S2");
        AuxiliaryUtils.addRowXS2(cfgS2);

        final SteerableParserIntegrator.Insert cfg178 = integrator.new Insert("Y178");
        AuxiliaryUtils.addRow1_(cfg178);

        final SteerableParserIntegrator.Insert cfg186 = integrator.new Insert("Y186");
        AuxiliaryUtils.addRow1_(cfg186);

        final SteerableParserIntegrator.Insert cfg190 = integrator.new Insert("Y190");
        AuxiliaryUtils.addRow1_(cfg190);

        final SteerableParserIntegrator.Insert cfg196 = integrator.new Insert("Y196");
        AuxiliaryUtils.addRow1_(cfg196);

        final SteerableParserIntegrator.Insert cfg198 = integrator.new Insert("Y198");
        AuxiliaryUtils.addRow1_(cfg198);

        final String splitType = integrator.getSplitType();

//        integrator.atpcoDownload();

        LineProcessor<Object> lineProcessor = new LineProcessor<Object>() {
            @Override
            public void doWith(String line, int lineNo, String fileName, Object global) throws Exception {
                char char0 = line.charAt(0);
                String date = integrator.getFileSelector().getOrder(fileName).substring(0, 6);
                if (char0 != 26) {
                    if (lineNo > 1) {
                        char char1 = line.charAt(1), char2 = line.charAt(2), char3 = line.charAt(3), char4 = line.charAt(4);
                        if (char0 == 'S') {
                            if (char1 == '1') {
                                AuxiliaryUtils.insertRowX1(line, lineNo, splitType, date, fileName, cfgS1);
                            } else if (char1 == '2') {
                                AuxiliaryUtils.insertRowXS2(line, lineNo, splitType, date, fileName, cfgS2);
                            }
                        } else if (char0 == '3' && char2 == '1') {
                            char action = line.charAt(1);
                            if (char3 == '7' && char4 == '8') {
                                AuxiliaryUtils.insertRow178(action, line, fileName, splitType, date, cfg178, "AIRPORT_CUSTOM_PARTITION_YQYR");
                            } else if (char3 == '8' && char4 == '6') {
                                AuxiliaryUtils.insertRow1_(action, line, fileName, splitType, date, cfg186);
                            } else if (char3 == '9') {
                                if (char4 == '0') {
                                    AuxiliaryUtils.insertRow1_(action, line, fileName, splitType, date, cfg190);
                                } else if (char4 == '6') {
                                    AuxiliaryUtils.insertRow1_(action, line, fileName, splitType, date, cfg196);
                                } else if (char4 == '8') {
                                    AuxiliaryUtils.insertRow1_(action, line, fileName, splitType, date, cfg198);
                                }
                            }
                        }
                    } else {
                        AuxiliaryUtils.insertRowHeader(line, splitType, fileName, cfgHeader);
                    }
                }
            }
        };
        integrator.unzipNoFile(new NoFileReaderHandler<Object>(lineProcessor));
    }
}
