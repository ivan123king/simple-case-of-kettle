package com.lw.kettle.trans.step;

import com.lw.kettle.utils.PrintUtil;
import lombok.extern.slf4j.Slf4j;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.RowAdapter;

/**
 * @author lw
 * @date 2021/11/25 0025
 * @description  表输出适配器
 */
@Slf4j
public class TableInputRowAdapter extends RowAdapter {

    private String adapterName;

    public TableInputRowAdapter(String adapterName){
        this.adapterName = adapterName;
    }

    @Override
    public void rowReadEvent(RowMetaInterface rowMeta, Object[] row) throws KettleStepException {
        /**
         * org.pentaho.di.trans.step.BaseStep#getRow()
         *      org.pentaho.di.trans.step.BaseStep#handleGetRow() 此处调用 rowReadEvent
         */
//        super.rowReadEvent(rowMeta, row);
        PrintUtil.printlnData(adapterName+"-->rowReadEvent",rowMeta,row);
    }

    @Override
    public void rowWrittenEvent(RowMetaInterface rowMeta, Object[] row) throws KettleStepException {
        /*
        org.pentaho.di.trans.step.BaseStep.putRow
                org.pentaho.di.trans.step.BaseStep.handlePutRow 此处调用 rowWrittenEvent
         */
//        super.rowWrittenEvent(rowMeta, row);
        PrintUtil.printlnData(adapterName+"-->rowWrittenEvent",rowMeta,row);
    }

    @Override
    public void errorRowWrittenEvent(RowMetaInterface rowMeta, Object[] row) throws KettleStepException {
        //当发生错误时，将错误数据打印出来
        PrintUtil.printlnData(adapterName+"-->errorRowWrittenEvent",rowMeta,row);
    }


}
