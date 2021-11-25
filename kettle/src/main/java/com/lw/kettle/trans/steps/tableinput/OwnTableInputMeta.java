package com.lw.kettle.trans.steps.tableinput;

import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.tableinput.TableInput;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;

/**
 * @author lw
 * @date 2021/11/25 0025
 * @description
 */
public class OwnTableInputMeta extends TableInputMeta {

    private RowAdapter rowAdapter;

    @Override
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr,
                                 TransMeta transMeta, Trans trans ) {
        return new OwnTableInput( stepMeta, stepDataInterface, cnr, transMeta, trans,rowAdapter);
    }

    public void setRowAdapter(RowAdapter rowAdapter) {
        this.rowAdapter = rowAdapter;
    }
}
