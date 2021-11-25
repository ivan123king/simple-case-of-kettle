package com.lw.kettle.trans.steps.tableinput;

import lombok.extern.slf4j.Slf4j;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.pentaho.di.trans.steps.tableinput.TableInput;
import org.pentaho.di.trans.steps.tableinput.TableInputData;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author lw
 * @date 2021/11/25 0025
 * @description
 */
public class OwnTableInput extends TableInput {

    private OwnTableInputMeta meta;

    private TableInputData data;

    public OwnTableInput(StepMeta stepMeta, StepDataInterface stepDataInterface,
                         int copyNr, TransMeta transMeta, Trans trans, RowAdapter rowAdapter) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);

        //添加行监控
        if (rowAdapter != null)
            addRowListener(rowAdapter);
    }

    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
        if (first) { // we just got started

            Object[] parameters;
            RowMetaInterface parametersMeta;
            first = false;

            // Make sure we read data from source steps...
            if (data.infoStream.getStepMeta() != null) {
                if (meta.isExecuteEachInputRow()) {
                    if (log.isDetailed()) {
                        logDetailed("Reading single row from stream [" + data.infoStream.getStepname() + "]");
                    }
                    data.rowSet = findInputRowSet(data.infoStream.getStepname());
                    if (data.rowSet == null) {
                        throw new KettleException("Unable to find rowset to read from, perhaps step ["
                                + data.infoStream.getStepname() + "] doesn't exist. (or perhaps you are trying a preview?)");
                    }
                    parameters = getRowFrom(data.rowSet);
                    parametersMeta = data.rowSet.getRowMeta();
                } else {
                    if (log.isDetailed()) {
                        logDetailed("Reading query parameters from stream [" + data.infoStream.getStepname() + "]");
                    }
                    RowMetaAndData rmad = readStartDate(); // Read values in lookup table (look)
                    parameters = rmad.getData();
                    parametersMeta = rmad.getRowMeta();
                }
                if (parameters != null) {
                    if (log.isDetailed()) {
                        logDetailed("Query parameters found = " + parametersMeta.getString(parameters));
                    }
                }
            } else {
                parameters = new Object[]{};
                parametersMeta = new RowMeta();
            }

            if (meta.isExecuteEachInputRow() && (parameters == null || parametersMeta.size() == 0)) {
                setOutputDone(); // signal end to receiver(s)
                return false; // stop immediately, nothing to do here.
            }

            boolean success = doQuery(parametersMeta, parameters);
            if (!success) {
                return false;
            }
        } else {
            if (data.thisrow != null) { // We can expect more rows

                try {
                    data.nextrow = data.db.getRow(data.rs, meta.isLazyConversionActive());
                } catch (KettleDatabaseException e) {
                    if (e.getCause() instanceof SQLException && isStopped()) {
                        //This exception indicates we tried reading a row after the statment for this step was cancelled
                        //this is expected and ok so do not pass the exception up
                        logDebug(e.getMessage());
                        return false;
                    } else {
                        throw e;
                    }
                }
                if (data.nextrow != null) {
                    incrementLinesInput();
                }
            }
        }

        if (data.thisrow == null) { // Finished reading?

            boolean done = false;
            if (meta.isExecuteEachInputRow()) { // Try to get another row from the input stream
                Object[] nextRow = getRowFrom(data.rowSet);
                if (nextRow == null) { // Nothing more to get!

                    done = true;
                } else {
                    // First close the previous query, otherwise we run out of cursors!
                    closePreviousQuery();

                    boolean success = doQuery(data.rowSet.getRowMeta(), nextRow); // OK, perform a new query
                    if (!success) {
                        return false;
                    }

                    if (data.thisrow != null) {
                        putRow(data.rowMeta, data.thisrow); // fill the rowset(s). (wait for empty)
                        data.thisrow = data.nextrow;

                        if (checkFeedback(getLinesInput())) {
                            if (log.isBasic()) {
                                logBasic("linenr " + getLinesInput());
                            }
                        }
                    }
                }
            } else {
                done = true;
            }

            if (done) {
                setOutputDone(); // signal end to receiver(s)
                return false; // end of data or error.
            }
        } else {
            putRow(data.rowMeta, data.thisrow); // fill the rowset(s). (wait for empty)
            data.thisrow = data.nextrow;

            if (checkFeedback(getLinesInput())) {
                if (log.isBasic()) {
                    logBasic("linenr " + getLinesInput());
                }
            }
        }

        return true;
    }

    @Override
    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        meta = (OwnTableInputMeta) smi;
        data = (TableInputData) sdi;

        return super.init(smi, sdi);
    }

    protected RowMetaAndData readStartDate() throws KettleException {
        if (log.isDetailed()) {
            logDetailed("Reading from step [" + data.infoStream.getStepname() + "]");
        }

        RowMetaInterface parametersMeta = new RowMeta();
        Object[] parametersData = new Object[]{};

        RowSet rowSet = findInputRowSet(data.infoStream.getStepname());
        if (rowSet != null) {
            Object[] rowData = getRowFrom(rowSet); // rows are originating from "lookup_from"
            while (rowData != null) {
                parametersData = RowDataUtil.addRowData(parametersData, parametersMeta.size(), rowData);
                parametersMeta.addRowMeta(rowSet.getRowMeta());

                rowData = getRowFrom(rowSet); // take all input rows if needed!
            }

            if (parametersMeta.size() == 0) {
                throw new KettleException("Expected to read parameters from step ["
                        + data.infoStream.getStepname() + "] but none were found.");
            }
        } else {
            throw new KettleException("Unable to find rowset to read from, perhaps step ["
                    + data.infoStream.getStepname() + "] doesn't exist. (or perhaps you are trying a preview?)");
        }

        RowMetaAndData parameters = new RowMetaAndData(parametersMeta, parametersData);

        return parameters;
    }

    protected boolean doQuery(RowMetaInterface parametersMeta, Object[] parameters) throws KettleDatabaseException {
        boolean success = true;

        // Open the query with the optional parameters received from the source steps.
        String sql = null;
        if (meta.isVariableReplacementActive()) {
            sql = environmentSubstitute(meta.getSQL());
        } else {
            sql = meta.getSQL();
        }

        if (log.isDetailed()) {
            logDetailed("SQL query : " + sql);
        }
        if (parametersMeta.isEmpty()) {
            data.rs = data.db.openQuery(sql, null, null, ResultSet.FETCH_FORWARD, meta.isLazyConversionActive());
        } else {
            data.rs =
                    data.db.openQuery(sql, parametersMeta, parameters, ResultSet.FETCH_FORWARD, meta
                            .isLazyConversionActive());
        }
        if (data.rs == null) {
            logError("Couldn't open Query [" + sql + "]");
            setErrors(1);
            stopAll();
            success = false;
        } else {
            // Keep the metadata
            data.rowMeta = data.db.getReturnRowMeta();

            // Set the origin on the row metadata...
            if (data.rowMeta != null) {
                for (ValueMetaInterface valueMeta : data.rowMeta.getValueMetaList()) {
                    valueMeta.setOrigin(getStepname());
                }
            }

            // Get the first row...
            data.thisrow = data.db.getRow(data.rs);
            if (data.thisrow != null) {
                incrementLinesInput();
                data.nextrow = data.db.getRow(data.rs);
                if (data.nextrow != null) {
                    incrementLinesInput();
                }
            }
        }
        return success;
    }

    private void closePreviousQuery() throws KettleDatabaseException {
        if (data.db != null) {
            data.db.closeQuery(data.rs);
        }
    }
}
