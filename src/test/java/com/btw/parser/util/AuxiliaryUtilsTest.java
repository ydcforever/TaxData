package com.btw.parser.util;

import com.fate.file.transfer.FileSelector;
import org.junit.Test;

public class AuxiliaryUtilsTest {

    @Test
    public void testServerDir() throws Exception {
        FileSelector fileSelector = new FileSelector("","");
        fileSelector.begin("1905202301").end("2007202301");
        String[] a = AuxiliaryUtils.serverAtpcoDir("/ATPCO/", fileSelector);
        for(String s : a) {
            System.out.println(s);
        }
    }
}