package com.btw.parser.service;

import com.btw.parser.util.AuxiliaryUtils;
import com.btw.parser.util.SteerableParserIntegrator;
import com.fate.file.parse.processor.LineProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

/**
 * Created by ydc on 2020/7/28.
 */
@Service
public class PFCParser {

    private JdbcTemplate jdbcTemplate;

    @Autowired
    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }


    public void doTask() throws Exception {
        final SteerableParserIntegrator integrator = new SteerableParserIntegrator(jdbcTemplate, "PFC");
        final SteerableParserIntegrator.Insert cfgC = integrator.new Insert("PFC_C");
        final SteerableParserIntegrator.Insert cfgE = integrator.new Insert("PFC_E");
        final SteerableParserIntegrator.Insert cfgP = integrator.new Insert("PFC_P");
        final SteerableParserIntegrator.Insert cfgQ = integrator.new Insert("PFC_Q");
        final SteerableParserIntegrator.Insert cfgT = integrator.new Insert("PFC_T");
        final SteerableParserIntegrator.Insert cfgX = integrator.new Insert("PFC_X");
        final String splitType = integrator.getSplitType();
//        integrator.download();
        LineProcessor<Object> lineProcessor = new LineProcessor<Object>() {
            @Override
            public void doWith(String line, int lineNo, String fileName, Object global) throws Exception {
//                List<FieldSpecification> row = SteerableLineProcessUtils.splitBySpecification(line, splitType, cfgC.getFieldSpecification());
//                row.add(new FieldSpecification("SOURCE_NAME", fileName));
//                cfgC.insertOne(row);
                AuxiliaryUtils.insertRowHeader(line, splitType, fileName, cfgC);
            }
        };
        integrator.parse(integrator.getUnzipDir(), lineProcessor, false);
    }
}
