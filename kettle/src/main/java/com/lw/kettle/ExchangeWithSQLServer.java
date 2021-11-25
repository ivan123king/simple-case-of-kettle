package com.lw.kettle;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.selectvalues.SelectValuesMeta;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;
import org.pentaho.di.trans.steps.tableoutput.TableOutputMeta;

import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author lw
 * @date 2021/9/7 0007
 * @description
 */
public class ExchangeWithSQLServer {
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
    public void test01() throws KettleException{
        long timeL = 1630737854000L;
        java.sql.Date date  = new java.sql.Date(timeL);

        Timestamp time = new Timestamp(timeL);

//        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(time);
    }

    @Test
    public void exchangeSqlServer2SqlServer() throws KettleException{
        //源数据库连接
        String src = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<connection>" +
                "<name>src</name>" +
                "<server>192.168.10.91</server>" +
                "<type>MSSQL</type>" +
                "<access>Native</access>" +
                "<database>test_2008</database>" +
                "<port>1433</port>" +
                "<username>sa</username>" +
                "<password>123456</password>" +
                "</connection>";

        //目标数据库连接
        String dest = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<connection>" +
                "<name>dest</name>" +
                "<server>192.168.90.42</server>" +
                "<type>Oracle</type>" +
                "<access>Native</access>" +
                "<database>orcl</database>" +
                "<port>1521</port>" +
                "<username>FABLE</username>" +
                "<password>FABLE</password>" +
                "</connection>";

        DatabaseMeta srcDatabaseMeta = new DatabaseMeta(src);
        DatabaseMeta destDatabaseMeta = new DatabaseMeta(dest);

        //创建转换元信息
        TransMeta transMeta = new TransMeta();
        transMeta.setName("sqlserver之间的交换");

        //设置源和目标
        transMeta.addDatabase(srcDatabaseMeta);
        transMeta.addDatabase(destDatabaseMeta);

		/*
		 * 创建  表输入->表输出
		 * 同时将两个步骤连接起来
		 */
        PluginRegistry registry = PluginRegistry.getInstance();
        TableInputMeta tableInputMeta = new TableInputMeta();
        String tableInputPluginId = registry.getPluginId(StepPluginType.class,
                tableInputMeta);

        tableInputMeta.setDatabaseMeta(srcDatabaseMeta);
        //设置查询条件
        String selectSql = "select datetime_2 from test_04";
        tableInputMeta.setSQL(selectSql);

        StepMeta tableInputStepMeta = new StepMeta(tableInputPluginId,
                "tableInput", (StepMetaInterface) tableInputMeta);
        transMeta.addStep(tableInputStepMeta);

        SelectValuesMeta selectValuesMeta = new SelectValuesMeta();
        String selectValuesPluginId = registry.getPluginId(
                StepPluginType.class, selectValuesMeta);
        selectValuesMeta.allocate(1,0,0);
        String[] srcColNames = new String[]{"datetime_2"};
        selectValuesMeta.setSelectName(srcColNames);
        String[] destColNames = new String[]{"DATE_1"};
        selectValuesMeta.setSelectRename(destColNames);

        StepMeta selectValuesStepMeta = new StepMeta(selectValuesPluginId,
                "selectValues", (StepMetaInterface) selectValuesMeta);
        transMeta.addStep(selectValuesStepMeta);

		/*
		 * 6. 添加hop把两个步骤关联起来
		 */
        transMeta.addTransHop(new TransHopMeta(tableInputStepMeta,
                selectValuesStepMeta));


        TableOutputMeta tableOutputMeta = new TableOutputMeta();
        tableOutputMeta.setDatabaseMeta(destDatabaseMeta);

        //设置目标表的 schema和表名
        tableOutputMeta.setSchemaName("FABLE");
        tableOutputMeta.setTablename("ORACLE_06");

        String tableOutputPluginId = registry.getPluginId(StepPluginType.class, tableOutputMeta);
        StepMeta tableOutputStep = new StepMeta(tableOutputPluginId, "tableOutput" , (StepMetaInterface) tableOutputMeta);


        //将步骤添加进去
        transMeta.addStep(tableOutputStep);

        //将步骤和上一步关联起来
        transMeta.addTransHop(new TransHopMeta(selectValuesStepMeta, tableOutputStep));

        Trans trans = new Trans(transMeta);

        //执行转换
        trans.execute(null);

        //等待完成
        trans.waitUntilFinished();

        trans.setLogLevel(LogLevel.DETAILED);

        if (trans.getErrors() > 0) {
            System.out.println("交换出错.");
            return;
        }

    }


    @Test
    public void exchangeSqlServer2Oracle() throws KettleException{
        //源数据库连接
        String src = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<connection>" +
                "<name>src</name>" +
                "<server>192.168.10.91</server>" +
                "<type>MSSQL</type>" +
                "<access>Native</access>" +
                "<database>test_2008</database>" +
                "<port>1433</port>" +
                "<username>sa</username>" +
                "<password>123456</password>" +
                "</connection>";

        //目标数据库连接
        String dest = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<connection>" +
                "<name>dest</name>" +
                "<server>192.168.90.42</server>" +
                "<type>Oracle</type>" +
                "<access>Native</access>" +
                "<database>orcl</database>" +
                "<port>1521</port>" +
                "<username>FABLE</username>" +
                "<password>FABLE</password>" +
                "</connection>";

        DatabaseMeta srcDatabaseMeta = new DatabaseMeta(src);
        DatabaseMeta destDatabaseMeta = new DatabaseMeta(dest);

        //创建转换元信息
        TransMeta transMeta = new TransMeta();
        transMeta.setName("sqlserver之间的交换");

        //设置源和目标
        transMeta.addDatabase(srcDatabaseMeta);
        transMeta.addDatabase(destDatabaseMeta);

		/*
		 * 创建  表输入->表输出
		 * 同时将两个步骤连接起来
		 */
        PluginRegistry registry = PluginRegistry.getInstance();
        TableInputMeta tableInputMeta = new TableInputMeta();
        String tableInputPluginId = registry.getPluginId(StepPluginType.class,
                tableInputMeta);

        tableInputMeta.setDatabaseMeta(srcDatabaseMeta);
        //设置查询条件
        String selectSql = "select datetime_2 from test_king_datetime";
        tableInputMeta.setSQL(selectSql);

        StepMeta tableInputStepMeta = new StepMeta(tableInputPluginId,
                "tableInput", (StepMetaInterface) tableInputMeta);
        transMeta.addStep(tableInputStepMeta);

        SelectValuesMeta selectValuesMeta = new SelectValuesMeta();
        String selectValuesPluginId = registry.getPluginId(
                StepPluginType.class, selectValuesMeta);
        selectValuesMeta.allocate(1,0,0);
        String[] srcColNames = new String[]{"datetime_2"};
        selectValuesMeta.setSelectName(srcColNames);
        String[] destColNames = new String[]{"DATE_1"};
        selectValuesMeta.setSelectRename(destColNames);

        StepMeta selectValuesStepMeta = new StepMeta(selectValuesPluginId,
                "selectValues", (StepMetaInterface) selectValuesMeta);
        transMeta.addStep(selectValuesStepMeta);

		/*
		 * 6. 添加hop把两个步骤关联起来
		 */
        transMeta.addTransHop(new TransHopMeta(tableInputStepMeta,
                selectValuesStepMeta));


        TableOutputMeta tableOutputMeta = new TableOutputMeta();
        tableOutputMeta.setDatabaseMeta(destDatabaseMeta);

        //设置目标表的 schema和表名
        tableOutputMeta.setSchemaName("FABLE");
        tableOutputMeta.setTablename("ORACLE_06");

        String tableOutputPluginId = registry.getPluginId(StepPluginType.class, tableOutputMeta);
        StepMeta tableOutputStep = new StepMeta(tableOutputPluginId, "tableOutput" , (StepMetaInterface) tableOutputMeta);


        //将步骤添加进去
        transMeta.addStep(tableOutputStep);

        //将步骤和上一步关联起来
        transMeta.addTransHop(new TransHopMeta(selectValuesStepMeta, tableOutputStep));

        Trans trans = new Trans(transMeta);

        //执行转换
        trans.execute(null);

        //等待完成
        trans.waitUntilFinished();

        trans.setLogLevel(LogLevel.DETAILED);

        if (trans.getErrors() > 0) {
            System.out.println("交换出错.");
            return;
        }

    }
}
