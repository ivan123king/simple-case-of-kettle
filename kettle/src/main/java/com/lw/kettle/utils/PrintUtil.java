package com.lw.kettle.utils;

import lombok.extern.slf4j.Slf4j;
import org.pentaho.di.core.row.RowMetaInterface;

/**
 * @author lw
 * @date 2021/11/25 0025
 * @description
 */
@Slf4j
public class PrintUtil {

    /**
     * 数据打印
     * @param dataType
     * @param rowMeta
     * @param row
     */
    public static void printlnData(String dataType, RowMetaInterface rowMeta, Object[] row){
        StringBuilder sb = new StringBuilder();
        //row.length是错误长度，不能用来表示  i< row.length
        for (int i = 0; i < rowMeta.size(); i++) {
            String name = rowMeta.getValueMeta(i).getName();
            String colType = rowMeta.getValueMeta(i).getOriginalColumnTypeName();

            sb.append(name + "[" + colType + "]=" + row[i] + ",");
        }
        if (sb.indexOf(",") > 0) {
            sb = sb.delete(sb.length()-1, sb.length());
        }
        log.info(dataType+"： " + sb.toString());
    }
}
