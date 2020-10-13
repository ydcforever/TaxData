package com.btw.parser.model;

import com.fate.annotation.LineFormat;
import com.fate.annotation.handler.LineFormatHandler;

/**
 * Created by ydc on 2020/7/22.
 */
public class TTBSX1 extends LineFormatHandler{

    @LineFormat(type = LineFormat.Format.FSE, s = 1, e = 2)
    private String recType;

    @LineFormat(type = LineFormat.Format.FSE, s = 3, e = 4)
    private String subCode;


    public TTBSX1(String line) throws Exception {
        super(line);
    }

    public String getRecType() {
        return recType;
    }

    public String getSubCode() {
        return subCode;
    }

    public static void main(String[] args) throws Exception {
        TTBSX1 ttbsx1 = new TTBSX1("2355446");
        System.out.println(ttbsx1.getSubCode());
    }
}
