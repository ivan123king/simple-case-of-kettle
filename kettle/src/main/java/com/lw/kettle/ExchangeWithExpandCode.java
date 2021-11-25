package com.lw.kettle;

import com.lw.kettle.trans.step.TableInputRowAdapter;
import com.lw.kettle.trans.step.TableOutputRowAdapter;
import com.lw.kettle.trans.steps.tableinput.OwnTableInputMeta;
import com.lw.kettle.trans.steps.tableoutput.OwnTableOutputMeta;
import com.sun.org.apache.xerces.internal.impl.xpath.XPath;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.RowAdapter;
import org.pentaho.di.trans.step.StepErrorMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;
import org.pentaho.di.trans.steps.tableoutput.TableOutputMeta;

import java.util.Map;

/**
 * @author lw
 * @date 2021/11/24 0024
 * @description
 */
@Slf4j
public class ExchangeWithExpandCode {

    @Before
    public void before() {
        try {
            // 初始化Kettle环境
            KettleEnvironment.init();
            EnvUtil.environmentInit();
        } catch (KettleException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void exchange()throws KettleException{
        TransMeta transMeta = new TransMeta();
        transMeta.setName("交换");

        PluginRegistry registry = PluginRegistry.getInstance();

        StepMeta inputStep = getTableInputStep(transMeta,registry);
        StepMeta outputStep = getTableOutputStep(transMeta,registry);
        StepMeta errorOutputStep = getTableOutputErrorStep(transMeta,registry);

        //错误数据处理
        VariableSpace space = new  Variables();
        StepErrorMeta errorMeta = new StepErrorMeta(space,outputStep,errorOutputStep);
        errorMeta.setEnabled(true);
        outputStep.setStepErrorMeta(errorMeta);
        errorOutputStep.setStepErrorMeta(errorMeta);

        Trans trans = new Trans(transMeta);

        transMeta.addTransHop(new TransHopMeta(inputStep, outputStep));
        transMeta.addTransHop(new TransHopMeta(outputStep, errorOutputStep));

		//执行转换
		trans.execute(null);

		//等待完成
		trans.waitUntilFinished();
		if (trans.getErrors() > 0) {
			System.out.println("交换出错.");
			return;
		}

//        StepMetaInterface tableOutputMeta = outputStep.getStepMetaInterface();
//        tableOutputMeta.getStep(outputStep,tableOutputMeta.getStepData(),1,transMeta,trans).addRowListener();
    }


    /**
     * 获取表输入
     * @param transMeta
     * @param registry
     * @return
     */
    public StepMeta getTableInputStep(TransMeta transMeta,PluginRegistry registry) throws KettleException{
        /*
		1. 源数据库连接
		 */
        String mysql_src = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<connection>" +
                "<name>src</name>" +
                "<server>192.168.10.64</server>" +
                "<type>MySQL</type>" +
                "<access>Native</access>" +
                "<database>test</database>" +
                "<port>3306</port>" +
                "<username>root</username>" +
                "<password>root</password>" +
                "</connection>";

        DatabaseMeta srcDatabaseMeta = new DatabaseMeta(mysql_src);

        transMeta.addDatabase(srcDatabaseMeta);

        OwnTableInputMeta tableInputMeta = new OwnTableInputMeta();

        tableInputMeta.setRowAdapter(new TableInputRowAdapter("tableInput"));

        String tableInputPluginId = registry.getPluginId(StepPluginType.class,
                tableInputMeta);

        tableInputMeta.setDatabaseMeta(srcDatabaseMeta);
        //设置查询条件
        String selectSql = "select id ,name from user_info_src";
        tableInputMeta.setSQL(selectSql);

        StepMeta tableInputStep = new StepMeta(tableInputPluginId,
                "tableInput", (StepMetaInterface) tableInputMeta);

        //给步骤添加在spoon工具中的显示位置
        tableInputStep.setDraw(true);
        tableInputStep.setLocation(100, 100);

        transMeta.addStep(tableInputStep);

        return tableInputStep;
    }

    /**
     * 表输出
     * @param transMeta
     * @param registry
     * @return
     * @throws KettleException
     */
    public StepMeta getTableOutputStep(TransMeta transMeta,PluginRegistry registry) throws KettleException{
        String mysql_dest = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<connection>" +
                "<name>dest</name>" +
                "<server>192.168.10.64</server>" +
                "<type>MySQL</type>" +
                "<access>Native</access>" +
                "<database>test</database>" +
                "<port>3306</port>" +
                "<username>root</username>" +
                "<password>root</password>" +
                "</connection>";
        DatabaseMeta destDatabaseMeta = new DatabaseMeta(mysql_dest);

        OwnTableOutputMeta tableOutputMeta = new OwnTableOutputMeta();

        tableOutputMeta.setRowAdapter(new TableOutputRowAdapter("tableOutput"));

        tableOutputMeta.setDatabaseMeta(destDatabaseMeta);

        tableOutputMeta.setSchemaName(null);
        tableOutputMeta.setTablename("user_info_dest");

        String tableOutputPluginId = registry.getPluginId(StepPluginType.class, tableOutputMeta);
        StepMeta tableOutputStep = new StepMeta(tableOutputPluginId, "tableOutput", (StepMetaInterface) tableOutputMeta);

        //将步骤添加进去
        transMeta.addStep(tableOutputStep);

        //给步骤添加在spoon工具中的显示位置
        tableOutputStep.setDraw(true);
        tableOutputStep.setLocation(200, 200);

        return tableOutputStep;
    }

    /**
     * 表输出
     * @param transMeta
     * @param registry
     * @return
     * @throws KettleException
     */
    public StepMeta getTableOutputErrorStep(TransMeta transMeta,PluginRegistry registry) throws KettleException{
        String mysql_dest = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<connection>" +
                "<name>dest</name>" +
                "<server>192.168.10.64</server>" +
                "<type>MySQL</type>" +
                "<access>Native</access>" +
                "<database>test</database>" +
                "<port>3306</port>" +
                "<username>root</username>" +
                "<password>root</password>" +
                "</connection>";
        DatabaseMeta destDatabaseMeta = new DatabaseMeta(mysql_dest);

        OwnTableOutputMeta tableOutputMeta = new OwnTableOutputMeta();

        tableOutputMeta.setRowAdapter(new TableOutputRowAdapter("tableErrorOutput"));

        tableOutputMeta.setDatabaseMeta(destDatabaseMeta);

        tableOutputMeta.setSchemaName(null);
        tableOutputMeta.setTablename("user_info_dest_error");

        String tableOutputPluginId = registry.getPluginId(StepPluginType.class, tableOutputMeta);
        StepMeta tableOutputStep = new StepMeta(tableOutputPluginId, "tableErrorOutput", (StepMetaInterface) tableOutputMeta);

        //将步骤添加进去
        transMeta.addStep(tableOutputStep);

        //给步骤添加在spoon工具中的显示位置
        tableOutputStep.setDraw(true);
        tableOutputStep.setLocation(300, 300);

        return tableOutputStep;
    }

}
