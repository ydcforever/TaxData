package com.btw.parser.service;

import com.btw.parser.util.AuxiliaryUtils;
import com.btw.parser.util.SteerableParserIntegrator;
import com.fate.file.parse.processor.LineProcessor;
import com.fate.file.parse.steerable.FieldSpecification;
import com.fate.file.parse.steerable.SteerableLineProcessUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Created by ydc on 2019/6/18.
 */
@Service
public class MPMParser {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    public void doTask() throws Exception {
        final SteerableParserIntegrator integrator = new SteerableParserIntegrator(jdbcTemplate, "MPM");
        final SteerableParserIntegrator.Insert headerConfig = integrator.new Insert("MPM_HEADER");
        AuxiliaryUtils.addSource(headerConfig);
        final Map<String, FieldSpecification> headerMap = headerConfig.getFieldSpecification();

        final SteerableParserIntegrator.Insert  config = integrator.new Insert("MPM");
        AuxiliaryUtils.addSource(config);
        final Map<String, FieldSpecification> cfgMap = config.getFieldSpecification();

        final String splitType = integrator.getSplitType();
//        integrator.download();
//        integrator.unzip();
        LineProcessor<Object> lineProcessor = new LineProcessor<Object>() {
            @Override
            public void doWith(String line, int lineNo, String fileName, Object global) throws Exception {
                if(lineNo == 1) {
                    SteerableLineProcessUtils.fillRow(line, splitType, headerMap);
                    AuxiliaryUtils.fillSource(headerMap, fileName);
                    headerConfig.insertOne(headerMap);
                } else {
                    SteerableLineProcessUtils.fillRow(line, splitType, cfgMap);
                    AuxiliaryUtils.fillSource(cfgMap, fileName);
                    config.insertOne(cfgMap);
                }
            }
        };
        integrator.parse(integrator.getUnzipDir(), lineProcessor, false);
    }
}
