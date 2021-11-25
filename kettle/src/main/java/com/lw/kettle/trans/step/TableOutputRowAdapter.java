package com.lw.kettle.trans.step;

import com.lw.kettle.utils.PrintUtil;
import lombok.extern.slf4j.Slf4j;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.RowAdapter;

/**
 * @author lw
 * @date 2021/11/25 0025
 * @description
 */
@Slf4j
public class TableOutputRowAdapter extends RowAdapter {

    private String adapterName;

    public TableOutputRowAdapter(String adapterName){
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
        /*
        定义了错误处理时会进入此处，如下文章定义错误处理
        https://blog.csdn.net/lw18751836671/article/details/121339655?spm=1001.2014.3001.5501

        org.pentaho.di.trans.step.BaseStep.putError
                org.pentaho.di.trans.step.BaseStep#handlePutError  此处调用 errorRowWrittenEvent

         */
        //当发生错误时，将错误数据打印出来
        PrintUtil.printlnData(adapterName+"-->errorRowWrittenEvent",rowMeta,row);

    }
}
