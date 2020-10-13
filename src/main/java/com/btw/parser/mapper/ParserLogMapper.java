package com.btw.parser.mapper;

import com.fate.log.IParserLog;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Created by ydc on 2020/8/24.
 */
@Repository
public interface ParserLogMapper extends IParserLog{

    public List<String> queryWXWarn(String fileType);

}
