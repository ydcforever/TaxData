package com.btw.parser.service;

import com.btw.parser.mapper.ParserLogMapper;
import com.btw.parser.util.SteerableParserIntegrator;
import com.fate.file.parse.batch.BatchPool;
import com.fate.file.parse.processor.LineProcessor;
import com.fate.file.parse.steerable.FieldSpecification;
import com.fate.file.parse.steerable.SteerableLineProcessUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Created by ydc on 2019/12/22.
 */
@Service
public class IcerParser {

    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ParserLogMapper logMapper;

    @Autowired
    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

//    @SteerableSchedule(id = "ICER", cron = "* 30 * * * ?")
//    @Transactional(isolation = Isolation.DEFAULT, propagation= Propagation.REQUIRED, rollbackFor=Exception.class)
    public void doTask() throws Exception {
        SteerableParserIntegrator integrator = new SteerableParserIntegrator(jdbcTemplate, "ICER").logMapper(logMapper);

        if(integrator.isValid()) {
            final SteerableParserIntegrator.Insert config = integrator.new Insert("ICER");
            final Map<String, FieldSpecification> row = config.getFieldSpecification();
            row.put("SOURCE_NAME", new FieldSpecification().define("SOURCE_NAME"));
                    //integrator.download();
            LineProcessor<Object> lineProcessor = new LineProcessor<Object>() {
                @Override
                public void doWith(String line, int lineNo, String fileName, Object global) throws Exception {
                    if(lineNo > 1) {
                        SteerableLineProcessUtils.fillRow(line, ",", row);
                        row.get("SOURCE_NAME").setVal(fileName);
                        config.insertOneWithUpdate(row);
                    }
                }
            };
            integrator.parse(integrator.getSaveDir(), lineProcessor, false);
        }
    }

    public void doBatch() throws Exception {
        SteerableParserIntegrator integrator = new SteerableParserIntegrator(jdbcTemplate, "ICER").logMapper(logMapper);
        if(integrator.isValid()) {
            final SteerableParserIntegrator.Insert config = integrator.new Insert("ICER");
            final Map<String, FieldSpecification> specification = config.getFieldSpecification();
            specification.put("SOURCE_NAME", new FieldSpecification().define("SOURCE_NAME"));

            final BatchPool<Map<String, FieldSpecification>> batchPool = config.getBatchInsert();
            batchPool.init(specification);

            integrator.download();
            LineProcessor<Object> lineProcessor = new LineProcessor<Object>() {
                @Override
                public void doWith(String line, int lineNo, String fileName, Object global) throws Exception {
                    if(lineNo > 1) {
                        Map<String, FieldSpecification> row = batchPool.getBatchRow();
                        SteerableLineProcessUtils.fillRow(line, ",", row);
                        row.get("SOURCE_NAME").setVal(fileName);
                        batchPool.tryBatch();
                    }
                }
            };
            integrator.parse(integrator.getSaveDir(), lineProcessor, false);
            batchPool.restBatch();
        }
    }
}
