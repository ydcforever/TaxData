package com.btw.parser.util;

import com.btw.parser.mapper.ParserLogMapper;
import com.fate.decompress.DecompressFile;
import com.fate.decompress.ReaderHandler;
import com.fate.decompress.UnzipFile;
import com.fate.file.parse.DBSteerableConfig;
import com.fate.file.parse.batch.BatchPool;
import com.fate.file.parse.processor.FileProcessor;
import com.fate.file.parse.processor.IFileProcessor;
import com.fate.file.parse.processor.LineProcessor;
import com.fate.file.parse.steerable.FieldSpecification;
import com.fate.file.transfer.FileSelector;
import com.fate.log.ParserLoggerProxy;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.File;
import java.util.Arrays;
import java.util.Map;

/**
 * Created by ydc on 2019/12/16.
 * 底层没有约束config的来源，无论是写硬码还是写配置文件或者读库都由用户决定。
 * SteerableParseIntegrator由用户自配，这里只是提供一些集成模板
 */
public class SteerableParserIntegrator {

    private JdbcTemplate jdbcTemplate;

    private String fileType;

    private String splitType;

    private String saveDir;

    private String unzipDir;

    private FileSelector fileSelector;

    private FTPFactory ftpFactory;

    private boolean valid = true;

    private DBSteerableConfig config;

    private ParserLogMapper logMapper = null;

    public SteerableParserIntegrator() {
    }

    public SteerableParserIntegrator(JdbcTemplate jdbcTemplate, String fileType) {
        this.jdbcTemplate = jdbcTemplate;
        this.fileType = fileType;
        this.config = new DBSteerableConfig(jdbcTemplate);
        Map<String, Object> info = config.queryFileStorage(fileType);
        if (info == null) {
            valid = false;
        } else {
            this.ftpFactory = new FTPFactory(info);
            this.splitType = info.get("PARSE_TYPE").toString();
            this.saveDir = info.get("SAVE_DIR").toString();
            Object unDir = info.get("UNZIP_DIR");
            if(unDir != null) {
                this.unzipDir = unDir.toString();
            }
            this.fileSelector = new FileSelector(info.get("FEATURE").toString(), info.get("REGEXP").toString());
            Object begin = info.get("BEGIN_FLAG");
            Object end = info.get("END_FLAG");
            if (begin != null) {
                this.fileSelector.begin(begin.toString());
            }
            if (end != null) {
                this.fileSelector.end(end.toString());
            }
        }
    }

    public SteerableParserIntegrator logMapper(ParserLogMapper logMapper) {
        this.logMapper = logMapper;
        return this;
    }

    public void download() throws Exception {
        ftpFactory.download(fileSelector);
    }

    //YQYR和TTBS下载
    public void atpcoDownload() throws Exception {
        String[] dir = AuxiliaryUtils.serverAtpcoDir(ftpFactory.getServerDir(), fileSelector);
        ftpFactory.download(fileSelector, dir);
    }

    public FTPFactory getFtpFactory() {
        return this.ftpFactory;
    }

    public void unzipNoFile(ReaderHandler handler) throws Exception {
        unzipNoFile(handler, false);
    }

    public void unzipNoFile(ReaderHandler handler, boolean delete) throws Exception {
        File[] files = new File(saveDir).listFiles();
        if (files != null) {
            Arrays.sort(files);
            WeChatFare weChatFare = new WeChatFare(logMapper);
            for (File file : files) {
                String name = file.getName();
                String order = fileSelector.getOrder(name);
                if (fileSelector.acceptFile(name) && fileSelector.acceptOrder(order)) {
                    DecompressFile decompressFile = new ParserLoggerProxy(logMapper, this.fileType, name, new UnzipFile()).parserSend(weChatFare).getTarget();
                    decompressFile.doWith(file, "", handler);
                    config.updateOrder(fileType, order);
                    if (delete) {
                        file.delete();
                    }
                }
            }
        }
    }

    /**
     * only when jdbcTemplate constructor
     *
     * @param dir
     * @param lineProcessor
     * @param delete
     */
    public <T> void parse(String dir, LineProcessor<T> lineProcessor, boolean delete) throws Exception{
        File[] files = new File(dir).listFiles();
        assert files != null;
        WeChatFare weChatFare = new WeChatFare(logMapper);
        for (File file : files) {
            Arrays.sort(files);
            String name = file.getName();
            String order = fileSelector.getOrder(name);
            if (fileSelector.acceptFile(name) && fileSelector.acceptOrder(order)) {
                IFileProcessor fileProcessor = new ParserLoggerProxy(logMapper, fileType, name, FileProcessor.getInstance()).parserSend(weChatFare).getTarget();
                fileProcessor.process(file, lineProcessor, 0, null);
                config.updateOrder(fileType, order);
                if (delete) {
                    file.delete();
                }
            }
        }
    }

    public SteerableParserIntegrator fileType(String fileType) {
        this.fileType = fileType;
        return this;
    }

    public SteerableParserIntegrator splitType(String splitType) {
        this.splitType = splitType;
        return this;
    }

    public SteerableParserIntegrator unzipDir(String unzipDir) {
        this.unzipDir = unzipDir;
        return this;
    }

    public SteerableParserIntegrator fileSelector(FileSelector fileSelector) {
        this.fileSelector = fileSelector;
        return this;
    }

    public String getSplitType() {
        return splitType;
    }

    public String getSaveDir() {
        return saveDir;
    }

    public String getUnzipDir() {
        return unzipDir;
    }

    public FileSelector getFileSelector() {
        return fileSelector;
    }

    public boolean isValid() {
        return valid;
    }

    public Map<String, FieldSpecification> getSpecification(String contextName){
        return config.loadTableStruct(contextName);
    }

    /**
     * 单文件可能对应多表，需要多个insert对象
     */
    public class Insert {

        private String tableName;

        private Map<String, FieldSpecification> specifications;

        public Insert() {
            this.tableName = config.queryTableName(fileType, fileType);
            this.specifications = config.loadTableStruct(fileType);
        }

        public Insert(String contextName) {
            this.tableName = config.queryTableName(fileType, contextName);
            this.specifications = config.loadTableStruct(contextName);
        }

        public BatchPool<Map<String, FieldSpecification>> getBatchInsert() {
            return getBatchInsert(2);
        }

        public BatchPool<Map<String, FieldSpecification>> getBatchInsert(int batchSize) {
            return config.createBatchPool(tableName, batchSize);
        }

        public void insertOne(Map<String, FieldSpecification> fieldSpecifications) throws DataAccessException {
            String sql = config.insertSqlGenerator(tableName, fieldSpecifications);
            jdbcTemplate.update(sql);
        }

        public void insertOneWithUpdate(Map<String, FieldSpecification> fieldSpecifications) throws DataAccessException {
            String sql = config.insertSqlGenerator(tableName, fieldSpecifications);
            try {
                jdbcTemplate.update(sql);
            } catch (DuplicateKeyException e) {
                String updateSql = config.updateSqlGenerator(tableName, fieldSpecifications);
                jdbcTemplate.update(updateSql);
            }
        }

        /**
         * 级联更新
         *
         * @param tableName
         * @param key
         * @param value
         */
        public void updateATPCO178(String tableName, String key, String value) throws DataAccessException {
            updateATPCO(key, value);
            FieldSpecification k = new FieldSpecification("CUSTOM_NO", key);
            FieldSpecification v = new FieldSpecification("EXPIRY_DATE", value, "D").df("yyMMdd");
            updateSingle(tableName, k, v);
        }

        /**
         * 辅表时间更新
         *
         * @param key
         * @param value
         */
        public void updateATPCO(String key, String value) throws DataAccessException {
            FieldSpecification k = new FieldSpecification("TBL_NO", key);
            FieldSpecification v = new FieldSpecification("EXPIRY_DATE", value, "D").df("yyMMdd");
            updateSingle(tableName, k, v);
        }

        /**
         * 键值更新
         *
         * @param key
         * @param value
         */
        private void updateSingle(String tableName, FieldSpecification key, FieldSpecification value) throws DataAccessException {
            StringBuilder sql = new StringBuilder("update ").append(tableName).append(" t set t.");
            sql.append(config.setVal(value)).append(" where t.").append(config.setVal(key))
                    .append(" and ").append(config.valGenerator(value))
                    .append(" between t.effective_date and t.expiry_date");
            jdbcTemplate.update(sql.toString());
        }

        public String getTableName() {
            return tableName;
        }

        public void addDefineField(FieldSpecification field){
            specifications.put(field.getCol(), field);
        }

        public Map<String, FieldSpecification> getFieldSpecification() {
            return specifications;
        }
    }
}

