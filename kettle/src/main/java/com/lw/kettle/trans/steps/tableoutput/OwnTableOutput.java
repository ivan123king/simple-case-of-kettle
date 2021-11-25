package com.lw.kettle.trans.steps.tableoutput;

import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.tableinput.TableInputData;
import org.pentaho.di.trans.steps.tableoutput.TableOutput;
import org.pentaho.di.trans.steps.tableoutput.TableOutputData;
import org.pentaho.di.trans.steps.tableoutput.TableOutputMeta;

/**
 * @author lw
 * @date 2021/11/25 0025
 * @description
 */
public class OwnTableOutput extends TableOutput {
    private static Class<?> PKG = OwnTableOutputMeta.class;

    private OwnTableOutputMeta meta;
    private TableOutputData data;

    public OwnTableOutput(StepMeta stepMeta, StepDataInterface stepDataInterface,
                          int copyNr, TransMeta transMeta, Trans trans, RowAdapter rowAdapter) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);

        //添加行监控
        if (rowAdapter != null)
            addRowListener(rowAdapter);
    }

    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
        meta = (OwnTableOutputMeta) smi;
        data = (TableOutputData) sdi;

        Object[] r = getRow(); // this also waits for a previous step to be finished.
        if (r == null) { // no more input to be expected...
            // truncate the table if there are no rows at all coming into this step
            if (first && meta.truncateTable()) {
                truncateTable();
            }
            return false;
        }

        if (first) {
            first = false;
            if (meta.truncateTable()) {
                truncateTable();
            }
            data.outputRowMeta = getInputRowMeta().clone();
            meta.getFields(data.outputRowMeta, getStepname(), null, null, this, repository, metaStore);

            if (!meta.specifyFields()) {
                // Just take the input row
                data.insertRowMeta = getInputRowMeta().clone();
            } else {

                data.insertRowMeta = new RowMeta();

                //
                // Cache the position of the compare fields in Row row
                //
                data.valuenrs = new int[meta.getFieldDatabase().length];
                for (int i = 0; i < meta.getFieldDatabase().length; i++) {
                    data.valuenrs[i] = getInputRowMeta().indexOfValue(meta.getFieldStream()[i]);
                    if (data.valuenrs[i] < 0) {
                        throw new KettleStepException(BaseMessages.getString(
                                PKG, "TableOutput.Exception.FieldRequired", meta.getFieldStream()[i]));
                    }
                }

                for (int i = 0; i < meta.getFieldDatabase().length; i++) {
                    ValueMetaInterface insValue = getInputRowMeta().searchValueMeta(meta.getFieldStream()[i]);
                    if (insValue != null) {
                        ValueMetaInterface insertValue = insValue.clone();
                        insertValue.setName(meta.getFieldDatabase()[i]);
                        data.insertRowMeta.addValueMeta(insertValue);
                    } else {
                        throw new KettleStepException(BaseMessages.getString(
                                PKG, "TableOutput.Exception.FailedToFindField", meta.getFieldStream()[i]));
                    }
                }
            }
        }

        try {
            Object[] outputRowData = writeToTable(getInputRowMeta(), r);
            if (outputRowData != null) {
                putRow(data.outputRowMeta, outputRowData); // in case we want it go further...
                incrementLinesOutput();
            }

            if (checkFeedback(getLinesRead())) {
                if (log.isBasic()) {
                    logBasic("linenr " + getLinesRead());
                }
            }
        } catch (KettleException e) {
            logError("Because of an error, this step can't continue: ", e);
            setErrors(1);
            stopAll();
            setOutputDone(); // signal end to receiver(s)
            return false;
        }

        return true;
    }

    protected void truncateTable() throws KettleDatabaseException {
        if (!meta.isPartitioningEnabled() && !meta.isTableNameInField()) {
            // Only the first one truncates in a non-partitioned step copy
            //
            if (meta.truncateTable()
                    && ((getCopy() == 0 && getUniqueStepNrAcrossSlaves() == 0) || !Utils.isEmpty(getPartitionID()))) {
                data.db.truncateTable(environmentSubstitute(meta.getSchemaName()), environmentSubstitute(meta
                        .getTableName()));

            }
        }
    }
}
