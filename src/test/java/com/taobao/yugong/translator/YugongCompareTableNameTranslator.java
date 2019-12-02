package com.taobao.yugong.translator;

import com.taobao.yugong.common.db.meta.ColumnMeta;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.model.record.Record;
import org.apache.commons.lang.ObjectUtils;

import java.sql.Types;
import java.util.Date;

/**
 * @author OsbornHu
 * @email hujianopp@163.com
 * @create 2019-11-06 17:45
 **/
public class YugongCompareTableNameTranslator extends AbstractDataTranslator implements DataTranslator {

    public boolean translator(Record record) {
        // 1. schema/table名不同
        record.setSchemaName("ifoc_test");
        record.setTableName("I1011N_TEST");

        return super.translator(record);
    }
}

