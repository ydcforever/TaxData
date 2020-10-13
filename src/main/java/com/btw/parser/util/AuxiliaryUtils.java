package com.btw.parser.util;

import com.fate.file.parse.steerable.FieldSpecification;
import com.fate.file.parse.steerable.SteerableLineProcessUtils;
import com.fate.file.transfer.FileSelector;
import org.springframework.dao.DataAccessException;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Map;

/**
 * Created by ydc on 2020/7/27.
 */
public final class AuxiliaryUtils {

    private static final String FIELD_SOURCE_NAME = "SOURCE_NAME";

    private static final String FIELD_LINE_NO = "LINE_NO";

    private static final String FIELD_EFFECTIVE_DATE  =  "EFFECTIVE_DATE";

    private static final String FIELD_TAX_AMOUNT  =  "TAX_AMOUNT";

    private static final String FIELD_S1_AMOUNT = "SERVICE_FEE_AMOUNT";

    private static final String FIELD_S1_DEC = "SERVICE_FEE_DEC";

    private static final String FIELD_X1_AMOUNT = "TAX_CALCULATION_AMOUNT";

    private static final String FIELD_X1_DEC = "TAX_CALCULATION_DEC";

    //X1
    public static void insertRowX1(String line, int lineNo, String splitType, String date, String filename, SteerableParserIntegrator.Insert insert) throws DataAccessException{
        Map<String, FieldSpecification> row = insert.getFieldSpecification();
        SteerableLineProcessUtils.fillRow(line, splitType, row);
        fillRowXS(row, lineNo, date, filename);
        fillX1TaxAmount(row);
        insert.insertOne(row);
    }

    //S1
    public static void insertRowS1(String line, int lineNo, String splitType, String date, String filename, SteerableParserIntegrator.Insert insert) throws DataAccessException{
        Map<String, FieldSpecification> row = insert.getFieldSpecification();
        SteerableLineProcessUtils.fillRow(line, splitType, row);
        fillRowXS(row, lineNo, date, filename);
        fillS1TaxAmount(row);
        insert.insertOne(row);
    }

    // X2/S2，附加 LINE_NO 和 FILE_DATE 列
    public static void insertRowXS2(String line, int lineNo, String splitType, String date, String filename, SteerableParserIntegrator.Insert insert) throws DataAccessException{
        Map<String, FieldSpecification> row = insert.getFieldSpecification();
        SteerableLineProcessUtils.fillRow(line, splitType, row);
        fillRowXS(row, lineNo, date, filename);
        insert.insertOne(row);
    }

    //除178子表插入，附加 LINE_NO 和 EXPIRY_DATE 列，更新截止期
    public static void insertRow1_(char action, String line, String filename, String splitType, String date, SteerableParserIntegrator.Insert insert) throws DataAccessException{
        if(action == '1') {
            String tblNo = SteerableLineProcessUtils.fixSubstring(line, 5, 13);
            insert.updateATPCO(tblNo, date);
        } else {
            Map<String, FieldSpecification> row = insert.getFieldSpecification();
            SteerableLineProcessUtils.fillRow(line, splitType, row);
            fillRow1_(row, filename, date);
            insert.insertOne(row);
        }
    }

    //178表插入，附加 LINE_NO 和 EXPIRY_DATE 列，更新截止期，并级联更新自生成表
    public static void insertRow178(char action, String line, String filename, String splitType, String date, SteerableParserIntegrator.Insert insert, String tableName)  throws DataAccessException{
        if(action == '1'){
            String tblNo = SteerableLineProcessUtils.fixSubstring(line, 5, 13);
            insert.updateATPCO178(tableName, tblNo, date);
        }else {
            Map<String, FieldSpecification> row = insert.getFieldSpecification();
            SteerableLineProcessUtils.fillRow(line, splitType, row);
            fillRow1_(row, filename, date);
            insert.insertOne(row);
        }
    }

    //Header
    public static void insertRowHeader(String line, String splitType, String fileName, SteerableParserIntegrator.Insert insert)  throws DataAccessException{
        Map<String, FieldSpecification> row = insert.getFieldSpecification();
        SteerableLineProcessUtils.fillRow(line, splitType, row);
        fillSource(row, fileName);
        insert.insertOne(row);
    }

    public static void fillRowXS(Map<String, FieldSpecification> row, int lineNo, String fileDate, String filename){
        fillLineNo(row, lineNo);
        fillSource(row, filename);
        fillEff(row, fileDate);
    }

    public static void fillRow1_(Map<String, FieldSpecification> row, String filename, String fileDate){
        fillSource(row, filename);
        fillEff(row, fileDate);
    }

    public static void addRowXS1(SteerableParserIntegrator.Insert insert){
        addRowXS2(insert);
        addTaxAmount(insert);
    }

    public static void addRowXS2(SteerableParserIntegrator.Insert insert){
        addLineNo(insert);
        addSource(insert);
        addEff(insert);
    }

    public static void addRow1_(SteerableParserIntegrator.Insert insert){
        addSource(insert);
        addEff(insert);
    }

    public static void addEff(SteerableParserIntegrator.Insert insert){
        insert.addDefineField(new FieldSpecification().define(FIELD_EFFECTIVE_DATE, "D").df("yyMMdd"));
    }

    public static void addSource(SteerableParserIntegrator.Insert insert){
        insert.addDefineField( new FieldSpecification().define(FIELD_SOURCE_NAME));
    }

    public static void addTaxAmount(SteerableParserIntegrator.Insert insert) {
        insert.addDefineField(new FieldSpecification().define(FIELD_TAX_AMOUNT, "N"));
    }

    public static void addLineNo(SteerableParserIntegrator.Insert insert) {
        insert.addDefineField(new FieldSpecification().define(FIELD_LINE_NO, "N"));
    }

    public static void fillEff(Map<String, FieldSpecification> row, String fileDate){
        row.get(FIELD_EFFECTIVE_DATE).setVal(fileDate);
    }

    public static void fillSource(Map<String, FieldSpecification> row, String filename){
        row.get(FIELD_SOURCE_NAME).setVal(filename);
    }

    public static void fillS1TaxAmount(Map<String, FieldSpecification> row) {
        FieldSpecification amount = row.get(FIELD_S1_AMOUNT);
        FieldSpecification dec = row.get(FIELD_S1_DEC);
        FieldSpecification taxAmount = row.get(FIELD_TAX_AMOUNT);
        taxAmount.setVal(getAmount(amount, dec));
    }

    public static void fillX1TaxAmount(Map<String, FieldSpecification> row) {
        FieldSpecification amount = row.get(FIELD_X1_AMOUNT);
        FieldSpecification dec = row.get(FIELD_X1_DEC);
        FieldSpecification taxAmount = row.get(FIELD_TAX_AMOUNT);
        taxAmount.setVal(getAmount(amount, dec));
    }

    public static void fillLineNo(Map<String, FieldSpecification> row, int lineNo){
        row.get(FIELD_LINE_NO).setVal(lineNo + "");
    }

    private static String getAmount(FieldSpecification amount, FieldSpecification dec){
        BigDecimal amt = new BigDecimal(amount.getVal()).divide(new BigDecimal("100000".substring(0, Integer.parseInt(dec.getVal()) + 1)));
        return amt.doubleValue() + "";
    }

    /**
     * TTBS和YQYR使用
     * @param root
     * @param fileSelector
     * @return
     */
    public static String[] serverAtpcoDir(String root, FileSelector fileSelector){
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(System.currentTimeMillis());

        String b = fileSelector.getBegin();
        int yearB = b == null ? 18 : Integer.parseInt(b.substring(0, 2));
        int mB = b == null ? 1 : Integer.parseInt(b.substring(2, 4));

        String e = fileSelector.getEnd();
        int yearE = e ==  null ? c.get(Calendar.YEAR) - 2000 : Integer.parseInt(e.substring(0, 2));
        int mE = e == null ? c.get(Calendar.MONTH) + 1 : Integer.parseInt(e.substring(2, 4));

        StringBuilder dir = new StringBuilder();
        for(int i = yearB; i <= yearE; i++) {
            StringBuilder year = new StringBuilder(root).append("20").append(i < 10 ? "0" + i : i).append("/");
            if (yearB == i && i < yearE) {
                //b月-12
                for(int k = mB; k <= 12; k++){
                    dir.append(year).append(k < 10 ? "0" + k : k).append("/;");
                }
            } else if(yearB == i && i == yearE) {
                //b月-e月
                for(int k = mB; k <= mE; k++){
                    dir.append(year).append(k < 10 ? "0" + k : k).append("/;");
                }
            } else if(yearB < i && i < yearE) {
                //1月-12月
                for(int k = 1; k <= 12; k++){
                    dir.append(year).append(k < 10 ? "0" + k : k).append("/;");
                }
            } else if(yearB < i && i == yearE) {
                //1月-e月
                for(int k = 1; k <= mE; k++){
                    dir.append(year).append(k < 10 ? "0" + k : k).append("/;");
                }
            }
        }
        return dir.toString().split(";");
    }
}

