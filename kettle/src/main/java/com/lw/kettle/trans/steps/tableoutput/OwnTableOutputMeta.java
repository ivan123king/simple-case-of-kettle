package com.lw.kettle.trans.steps.tableoutput;

import lombok.Data;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.tableoutput.TableOutputMeta;

/**
 * @author lw
 * @date 2021/11/25 0025
 * @description
 */
public class OwnTableOutputMeta extends TableOutputMeta {

    private RowAdapter rowAdapter;

    @Override
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta, Trans trans) {
        return new OwnTableOutput(stepMeta, stepDataInterface, cnr, transMeta, trans,rowAdapter);
    }

    public void setRowAdapter(RowAdapter rowAdapter) {
        this.rowAdapter = rowAdapter;
    }
}
