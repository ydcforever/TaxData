package com.btw.parser.service;

import com.btw.parser.util.AuxiliaryUtils;
import com.btw.parser.util.SteerableParserIntegrator;
import com.fate.file.parse.processor.LineProcessor;
import com.fate.file.parse.steerable.FieldSpecification;
import com.fate.file.parse.steerable.SteerableLineProcessUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
* Created by ydc on 2019/7/1.
*/
@Service
public class TPMParser {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private boolean isSector(String line) {
        return line.charAt(2) == ' ';
    }

    public void doTask() throws Exception {
        SteerableParserIntegrator integrator = new SteerableParserIntegrator(jdbcTemplate, "TPMBULL");
        final SteerableParserIntegrator.Insert headerConfig = integrator.new Insert("TPM_HEADER");
        AuxiliaryUtils.addSource(headerConfig);
        final Map<String, FieldSpecification> headerMap = headerConfig.getFieldSpecification();

        final SteerableParserIntegrator.Insert config = integrator.new Insert("TPM");
        AuxiliaryUtils.addSource(config);
        final Map<String, FieldSpecification> cfgMap = headerConfig.getFieldSpecification();

        final Map<String, FieldSpecification> list = integrator.getSpecification("TPM_LINE");
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
                    if(isSector(line)) {
                        SteerableLineProcessUtils.fillRow(line, splitType, cfgMap);
                        AuxiliaryUtils.fillSource(cfgMap, fileName);
                        Map<String, FieldSpecification> f = new HashMap<>();
                        f.putAll(cfgMap);
                        f.putAll(list);
                        config.insertOne(f);
                    } else {
                       SteerableLineProcessUtils.fillRow(line, splitType, list);
                    }
                }
            }
        };
        integrator.parse(integrator.getUnzipDir(), lineProcessor, false);
    }
}
