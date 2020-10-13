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
 * Created by ydc on 2019/12/26.
 */
@Service
public class TTBSParser {

    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ParserLogMapper logMapper;

    private static final String FIELD_SOURCE_NAME = "SOURCE_NAME";

    @Autowired
    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void doTask() throws Exception {
        final SteerableParserIntegrator integrator = new SteerableParserIntegrator(jdbcTemplate, "TTBS").logMapper(logMapper);
        final String splitType = integrator.getSplitType();
        final SteerableParserIntegrator.Insert cfgHeader = integrator.new Insert("T_HEADER");
        AuxiliaryUtils.addSource(cfgHeader);

        final SteerableParserIntegrator.Insert cfgX1 = integrator.new Insert("X1");
        AuxiliaryUtils.addRowXS1(cfgX1);

        final SteerableParserIntegrator.Insert cfgX2 = integrator.new Insert("X2");
        AuxiliaryUtils.addRowXS2(cfgX2);

        final SteerableParserIntegrator.Insert cfg164 = integrator.new Insert("T164");
        AuxiliaryUtils.addRow1_(cfg164);

        final SteerableParserIntegrator.Insert cfg167 = integrator.new Insert("T167");
        AuxiliaryUtils.addRow1_(cfg167);

        final SteerableParserIntegrator.Insert cfg168 = integrator.new Insert("T168");
        AuxiliaryUtils.addRow1_(cfg168);

        final SteerableParserIntegrator.Insert cfg169 = integrator.new Insert("T169");
        AuxiliaryUtils.addRow1_(cfg169);

        final SteerableParserIntegrator.Insert cfg178 = integrator.new Insert("T178");
        AuxiliaryUtils.addRow1_(cfg178);

        final SteerableParserIntegrator.Insert cfg183 = integrator.new Insert("T183");
        AuxiliaryUtils.addRow1_(cfg183);

        final SteerableParserIntegrator.Insert cfg186 = integrator.new Insert("T186");
        AuxiliaryUtils.addRow1_(cfg186);

        final SteerableParserIntegrator.Insert cfg190 = integrator.new Insert("T190");
        AuxiliaryUtils.addRow1_(cfg190);

//        integrator.atpcoDownload();
//
        LineProcessor<Object> lineProcessor = new LineProcessor<Object>() {
            @Override
            public void doWith(String line, int lineNo, String fileName, Object global) throws Exception {
                String date = integrator.getFileSelector().getOrder(fileName).substring(0, 6);
                char a = line.charAt(0);
                if (lineNo > 1) {
                    if ('X' == a) {
                        // 进入 atMainForm_1|2 细分过程
                        char a1 = line.charAt(1);
                        if ('1' == a1) {
                            AuxiliaryUtils.insertRowS1(line, lineNo, splitType, date, fileName, cfgX1);
                        } else if ('2' == a1) {
                            AuxiliaryUtils.insertRowXS2(line, lineNo, splitType, date, fileName, cfgX2);
                        }
                    } else if ('3' == a) {
                        // 进入 atChildrenForm 细分过程
                        char action = line.charAt(1);
                        char b = line.charAt(2);
                        if ('1' == b) {
                            // 进入 atChildrenForm_164|167|168|169|178|183|186|190 细分过程
                            char c = line.charAt(3);
                            char d = line.charAt(4);
                            if ('6' == c) {
                                // 进入 atChildrenForm_164|167|168|169 细分过程
                                if ('4' == d) {
                                    AuxiliaryUtils.insertRow1_(action, line, fileName, splitType, date, cfg164);
                                } else if ('7' == d) {
                                    AuxiliaryUtils.insertRow1_(action, line, fileName, splitType, date, cfg167);
                                } else if ('8' == d) {
                                    AuxiliaryUtils.insertRow1_(action, line, fileName, splitType, date, cfg168);
                                } else if ('9' == d) {
                                    AuxiliaryUtils.insertRow1_(action, line, fileName, splitType, date, cfg169);
                                }
                            } else if ('7' == c && '8' == d) {
                                AuxiliaryUtils.insertRow178(action, line, fileName, splitType, date, cfg178, "AIRPORT_CUSTOM_PARTITION_TTBS");
                            } else if ('8' == c) {
                                if ('3' == d) {
                                    AuxiliaryUtils.insertRow1_(action, line, fileName, splitType, date, cfg183);
                                } else if ('6' == d) {
                                    AuxiliaryUtils.insertRow1_(action, line, fileName, splitType, date, cfg186);
                                }
                            } else if ('9' == c && '0' == d) {
                                AuxiliaryUtils.insertRow1_(action, line, fileName, splitType, date, cfg190);
                            }
                        }
                    }
                } else {
                    AuxiliaryUtils.insertRowHeader(line, splitType, fileName, cfgHeader);
                }
            }
        };
        integrator.unzipNoFile(new NoFileReaderHandler<Object>(lineProcessor));
    }
}
