package com.lw.kettle;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepErrorMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.jsonoutput.JsonOutput;
import org.pentaho.di.trans.steps.selectvalues.SelectValuesMeta;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;
import org.pentaho.di.trans.steps.tableoutput.TableOutputMeta;

/**
 * 关联mysql相关的交换
 * @author Administrator
 *
 */
public class ExchangeWithMySQL {

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


	/**
	 * 错误处理
	 * @throws KettleException
	 */
	@Test
	public void exchangeMysql2Mysql() throws KettleException{
		/*
		1. 源数据库连接
		 */
		String mysql_src = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<connection>" +
                "<name>mysql_src</name>" +
                "<server>192.168.10.64</server>" +
                "<type>MySQL</type>" +
                "<access>Native</access>" +
                "<database>test</database>" +
                "<port>3306</port>" +
                "<username>root</username>" +
                "<password>root</password>" +
                "</connection>";

		/*
		2. 目标数据库连接
		 */
        String mysql_dest = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<connection>" +
                "<name>mysql_dest</name>" +
				"<server>192.168.10.64</server>" +
				"<type>MySQL</type>" +
				"<access>Native</access>" +
				"<database>test</database>" +
				"<port>3306</port>" +
				"<username>root</username>" +
				"<password>root</password>" +
				"</connection>";

        DatabaseMeta srcDatabaseMeta = new DatabaseMeta(mysql_src);
        DatabaseMeta destDatabaseMeta = new DatabaseMeta(mysql_dest);

        //创建转换元信息
        TransMeta transMeta = new TransMeta();
		transMeta.setName("mysql8之间的交换");

		//设置源和目标
		transMeta.addDatabase(srcDatabaseMeta);
		transMeta.addDatabase(destDatabaseMeta);

		/*
		 * 创建  表输入->选择-->表输出/错误输出
		 * 同时将两个步骤连接起来
		 */
		PluginRegistry registry = PluginRegistry.getInstance();

		/*
		3. 表输入
		 */
		TableInputMeta tableInputMeta = new TableInputMeta();
		String tableInputPluginId = registry.getPluginId(StepPluginType.class,
				tableInputMeta);

		tableInputMeta.setDatabaseMeta(srcDatabaseMeta);
		//设置查询条件
		String selectSql = "select id ,name from user_info_src";
		tableInputMeta.setSQL(selectSql);

		StepMeta tableInputStepMeta = new StepMeta(tableInputPluginId,
				"tableInput", (StepMetaInterface) tableInputMeta);

		//给步骤添加在spoon工具中的显示位置
		tableInputStepMeta.setDraw(true);
		tableInputStepMeta.setLocation(100, 100);

		transMeta.addStep(tableInputStepMeta);

		/*
		4. 定义字段选择器
		 */
		SelectValuesMeta selectValuesMeta = new SelectValuesMeta();
		selectValuesMeta.allocate(2,0,0);

		String[] fields = new String[]{"id","name"};
		selectValuesMeta.setSelectName(fields);
		selectValuesMeta.setSelectRename(fields);

		String selectPluginId = registry.getPluginId(StepPluginType.class, selectValuesMeta);
		StepMeta selectStep = new StepMeta(selectPluginId, "select", (StepMetaInterface) selectValuesMeta);

		//给步骤添加在spoon工具中的显示位置
		selectStep.setDraw(true);
		selectStep.setLocation(100, 200);

		//将步骤添加进去
		transMeta.addStep(selectStep);

		transMeta.addTransHop(new TransHopMeta(tableInputStepMeta, selectStep));

		/*
		5. 定义错误输出
		 */
		TableOutputMeta tableOutputErrorMeta = new TableOutputMeta();
		tableOutputErrorMeta.setDatabaseMeta(destDatabaseMeta);

		//设置目标表的 schema和表名
		tableOutputErrorMeta.setSchemaName(null);
		tableOutputErrorMeta.setTablename("user_info_dest_error");

		String tableOutputErrorPluginId = registry.getPluginId(StepPluginType.class, tableOutputErrorMeta);
		StepMeta tableOutputErrorStep = new StepMeta(tableOutputErrorPluginId, "tableOutputError", (StepMetaInterface) tableOutputErrorMeta);

		//将步骤添加进去
		transMeta.addStep(tableOutputErrorStep);

		//给步骤添加在spoon工具中的显示位置
		tableOutputErrorStep.setDraw(true);
		tableOutputErrorStep.setLocation(300, 300);


		/*
		6. 定义正常输出
		 */
		TableOutputMeta tableOutputMeta = new TableOutputMeta();
		tableOutputMeta.setDatabaseMeta(destDatabaseMeta);

		//设置目标表的 schema和表名
		tableOutputMeta.setSchemaName(null);
		tableOutputMeta.setTablename("user_info_dest");

		String tableOutputPluginId = registry.getPluginId(StepPluginType.class, tableOutputMeta);
		StepMeta tableOutputStep = new StepMeta(tableOutputPluginId, "tableOutput", (StepMetaInterface) tableOutputMeta);

		//将步骤添加进去
		transMeta.addStep(tableOutputStep);

		//给步骤添加在spoon工具中的显示位置
		tableOutputStep.setDraw(true);
		tableOutputStep.setLocation(200, 200);

		StepErrorMeta stepErrorMeta = new StepErrorMeta(null,selectStep,tableOutputErrorStep);
		stepErrorMeta.setEnabled(true);
		selectStep.setStepErrorMeta(stepErrorMeta);
		tableOutputErrorStep.setStepErrorMeta(stepErrorMeta);


		/*
		7. 将步骤关联
		 */
		transMeta.addTransHop(new TransHopMeta(selectStep, tableOutputStep));
		transMeta.addTransHop(new TransHopMeta(selectStep, tableOutputErrorStep));



		String xml = transMeta.getXML();
		System.out.println(xml);


//		Trans trans = new Trans(transMeta);
//
//
//		//执行转换
//		trans.execute(null);
//
//		//等待完成
//		trans.waitUntilFinished();
//		if (trans.getErrors() > 0) {
//			System.out.println("交换出错.");
//			return;
//		}

	}


	/**
	 * Mysql之间的交换
	 * @throws KettleException
	 */
	@Test
	public void exchangeMysql2MysqlErrorStep() throws KettleException{
		/*
		1. 源数据库连接
		 */
		String mysql_src = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
				"<connection>" +
				"<name>mysql_src</name>" +
				"<server>192.168.10.64</server>" +
				"<type>MySQL</type>" +
				"<access>Native</access>" +
				"<database>test</database>" +
				"<port>3306</port>" +
				"<username>root</username>" +
				"<password>root</password>" +
				"</connection>";

		/*
		2. 目标数据库连接
		 */
		String mysql_dest = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
				"<connection>" +
				"<name>mysql_dest</name>" +
				"<server>192.168.10.64</server>" +
				"<type>MySQL</type>" +
				"<access>Native</access>" +
				"<database>test</database>" +
				"<port>3306</port>" +
				"<username>root</username>" +
				"<password>root</password>" +
				"</connection>";

		DatabaseMeta srcDatabaseMeta = new DatabaseMeta(mysql_src);
		DatabaseMeta destDatabaseMeta = new DatabaseMeta(mysql_dest);

		//创建转换元信息
		TransMeta transMeta = new TransMeta();
		transMeta.setName("mysql8之间的交换");

		//设置源和目标
		transMeta.addDatabase(srcDatabaseMeta);
		transMeta.addDatabase(destDatabaseMeta);

		/*
		 * 创建  表输入->选择-->表输出/错误输出
		 * 同时将两个步骤连接起来
		 */
		PluginRegistry registry = PluginRegistry.getInstance();

		/*
		3. 表输入
		 */
		TableInputMeta tableInputMeta = new TableInputMeta();
		String tableInputPluginId = registry.getPluginId(StepPluginType.class,
				tableInputMeta);

		tableInputMeta.setDatabaseMeta(srcDatabaseMeta);
		//设置查询条件
		String selectSql = "select id ,name from user_info_src";
		tableInputMeta.setSQL(selectSql);

		StepMeta tableInputStepMeta = new StepMeta(tableInputPluginId,
				"tableInput", (StepMetaInterface) tableInputMeta);

		//给步骤添加在spoon工具中的显示位置
		tableInputStepMeta.setDraw(true);
		tableInputStepMeta.setLocation(100, 100);

		transMeta.addStep(tableInputStepMeta);


		/*
		4. 定义正常输出
		 */
		TableOutputMeta tableOutputMeta = new TableOutputMeta();
		tableOutputMeta.setDatabaseMeta(destDatabaseMeta);

		//设置目标表的 schema和表名
		tableOutputMeta.setSchemaName(null);
		tableOutputMeta.setTablename("user_info_dest");

		String tableOutputPluginId = registry.getPluginId(StepPluginType.class, tableOutputMeta);
		StepMeta tableOutputStep = new StepMeta(tableOutputPluginId, "tableOutput", (StepMetaInterface) tableOutputMeta);

		//将步骤添加进去
		transMeta.addStep(tableOutputStep);
		transMeta.addTransHop(new TransHopMeta(tableInputStepMeta, tableOutputStep));

		//给步骤添加在spoon工具中的显示位置
		tableOutputStep.setDraw(true);
		tableOutputStep.setLocation(200, 200);

		/*
		5. 定义错误输出
		 */
		TableOutputMeta tableOutputErrorMeta = new TableOutputMeta();
		tableOutputErrorMeta.setDatabaseMeta(destDatabaseMeta);

		//设置目标表的 schema和表名
		tableOutputErrorMeta.setSchemaName(null);
		tableOutputErrorMeta.setTablename("user_info_dest_error");

		String tableOutputErrorPluginId = registry.getPluginId(StepPluginType.class, tableOutputErrorMeta);
		StepMeta tableOutputErrorStep = new StepMeta(tableOutputErrorPluginId, "tableOutputError", (StepMetaInterface) tableOutputErrorMeta);

		//将步骤添加进去
		transMeta.addStep(tableOutputErrorStep);
		transMeta.addTransHop(new TransHopMeta(tableOutputStep, tableOutputErrorStep));

		//给步骤添加在spoon工具中的显示位置
		tableOutputErrorStep.setDraw(true);
		tableOutputErrorStep.setLocation(300, 300);

		/*
		6.定义错误数据流输出
		 */
		StepErrorMeta stepErrorMeta = new StepErrorMeta(null,tableOutputStep,tableOutputErrorStep);
		stepErrorMeta.setEnabled(true);
		tableOutputStep.setStepErrorMeta(stepErrorMeta);
		tableOutputErrorStep.setStepErrorMeta(stepErrorMeta);


		/*
		7. 输出xml，在spoon中展示图形化
		 */
		String xml = transMeta.getXML();
		System.out.println(xml);
	}
}
