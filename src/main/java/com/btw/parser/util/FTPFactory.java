package com.btw.parser.util;

import com.fate.file.transfer.FTPAccessor;
import com.fate.file.transfer.FileSelector;
import org.springframework.beans.factory.InitializingBean;

import java.io.File;
import java.util.Map;

/**
 * Created by ydc on 2020/7/22.
 */
public class FTPFactory implements InitializingBean {

    private String username;

    private String password;

    private String host;

    private int port;

    private String serverDir;

    private String saveDir;

    public FTPFactory(String username, String password, String host, int port, String serverDir, String saveDir) {
        this.username = username;
        this.password = password;
        this.host = host;
        this.port = port;
        this.serverDir = serverDir;
        this.saveDir = saveDir;
    }

    public FTPFactory(Map<String, Object> params) {
        this.username = params.get("USER_NAME").toString();
        this.password = params.get("PSWD").toString();
        this.host = params.get("ADDR").toString();
        this.port = Integer.parseInt(params.get("PORT").toString());
        this.serverDir = params.get("SERVER_DIR").toString();
        this.saveDir = params.get("SAVE_DIR").toString();
    }


    public void download(FileSelector fileSelector) throws Exception {
        FTPAccessor.accessWithFtpFileProcessor(host, port, username, password, fileSelector, saveDir, true, serverDir);
    }

    public String getServerDir() {
        return serverDir;
    }

    public void download(FileSelector fileSelector, String... ftpDir) throws Exception {
        FTPAccessor.accessWithFtpFileProcessor(host, port, username, password, fileSelector, saveDir, true, ftpDir);
    }

    public void download(FTPAccessor.FTPFileProcessor ftpFileProcessor) throws Exception{
        FTPAccessor.access(host, port, username, password, ftpFileProcessor, saveDir, true, serverDir);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        checkSaveDir(this.saveDir);
    }

    private void checkSaveDir(String path){
        File file = new File(path);
        if(!file.exists() || !file.isDirectory()){
            file.mkdir();
        }
    }
}
