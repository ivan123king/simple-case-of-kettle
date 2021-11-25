package com.lw.kettle;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.rowstoresult.RowsToResultMeta;
import org.pentaho.di.trans.steps.selectvalues.SelectMetadataChange;
import org.pentaho.di.trans.steps.selectvalues.SelectValuesMeta;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;
import org.pentaho.di.trans.steps.tableoutput.TableOutputData;
import org.pentaho.di.trans.steps.tableoutput.TableOutputMeta;

import java.util.ArrayList;
import java.util.List;

/**
 * 关联hive相关的交换
 * @author Administrator
 *
 */
public class ExchangeWithHive {
	
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
	 * hive连接
	 * @throws KettleException
	 */
	@Test
	public void hiveConnect() throws KettleException{
		//源数据库连接
		String hive_src = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
				"<connection>" +
				"<name>hive_src</name>" +
				"<server>192.168.10.212</server>" +
				"<type>HIVE</type>" +
				"<access>Native</access>" +
				"<database>ntzw_dev_64</database>" +
				"<port>10000</port>" +
				"<username>hadoop</username>" +
				"<password>hadoop</password>" +
				"</connection>";


		DatabaseMeta srcDatabaseMeta = new DatabaseMeta(hive_src);

		Database db = new Database(null,srcDatabaseMeta);

		db.connect();

		System.out.println("OK");

		db.disconnect();

	}
	
	/**
	 * hive之间的交换
	 * @throws KettleException 
	 */
	@Test
	public void exchangeHive2Hive() throws KettleException{
		//源数据库连接
		String hive_src = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<connection>" +
                "<name>hive_src</name>" +
                "<server>192.168.10.212</server>" +
                "<type>HIVE2</type>" +
                "<access>Native</access>" +
                "<database>ntzw_dev_64</database>" +
                "<port>10000</port>" +
                "<username>hadoop</username>" +
                "<password>hadoop</password>" +
                "</connection>";
		
		//目标数据库连接
        String hive_dest = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                "<connection>" +
                "<name>hive_dest</name>" +
                "<server>192.168.10.212</server>" +
                "<type>HIVE2</type>" +
                "<access>Native</access>" +
                "<database>ntzw_dev_64</database>" +
                "<port>10000</port>" +
                "<username>hadoop</username>" +
                "<password>hadoop</password>" +
                "</connection>";
        
        DatabaseMeta srcDatabaseMeta = new DatabaseMeta(hive_src);
        DatabaseMeta destDatabaseMeta = new DatabaseMeta(hive_dest);
        
        //创建转换元信息
        TransMeta transMeta = new TransMeta();
		transMeta.setName("hive之间的交换");
		
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
		String selectSql = "select id ,name from user_info_src";
		tableInputMeta.setSQL(selectSql);
		
		StepMeta tableInputStepMeta = new StepMeta(tableInputPluginId,
				"tableInput", (StepMetaInterface) tableInputMeta);
		transMeta.addStep(tableInputStepMeta);
		
		TableOutputMeta tableOutputMeta = new TableOutputMeta();
		tableOutputMeta.setDatabaseMeta(destDatabaseMeta);
		
		//设置目标表的 schema和表名
		tableOutputMeta.setSchemaName(null);
		tableOutputMeta.setTablename("user_info_dest");
		
		String tableOutputPluginId = registry.getPluginId(StepPluginType.class, tableOutputMeta);
		StepMeta tableOutputStep = new StepMeta(tableOutputPluginId, "tableOutput" , (StepMetaInterface) tableOutputMeta);
		
		//将步骤添加进去
		transMeta.addStep(tableOutputStep);
		
		//将步骤和上一步关联起来
		transMeta.addTransHop(new TransHopMeta(tableInputStepMeta, tableOutputStep));
		
		Trans trans = new Trans(transMeta);

		//执行转换
		trans.execute(null);
		
		//等待完成
		trans.waitUntilFinished();
		if (trans.getErrors() > 0) {
			System.out.println("交换出错.");
			return;
		}
        
	}

	/**
	 * mysql到hive之间的交换
	 * @throws KettleException
	 */
	@Test
	public void exchangeMySQL2Hive() throws KettleException{
		//源数据库连接
		String mysqL_src = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
				"<connection>" +
				"<name>mysqL_src</name>" +
				"<server>192.168.10.64</server>" +
				"<type>MYSQL</type>" +
				"<access>Native</access>" +
				"<database>test</database>" +
				"<port>3306</port>" +
				"<username>root</username>" +
				"<password>root</password>" +
				"</connection>";

		//目标数据库连接
		String hive_dest = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
				"<connection>" +
				"<name>hive_dest</name>" +
				"<server>192.168.10.212</server>" +
				"<type>HIVE</type>" +
				"<access>Native</access>" +
				"<database>ntzw_dev_64</database>" +
				"<port>10000</port>" +
				"<username>hadoop</username>" +
				"<password>hadoop</password>" +
				"</connection>";

		DatabaseMeta srcDatabaseMeta = new DatabaseMeta(mysqL_src);
		DatabaseMeta destDatabaseMeta = new DatabaseMeta(hive_dest);

		//创建转换元信息
		TransMeta transMeta = new TransMeta();
		transMeta.setName("mysql到hive之间的交换");

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
		String selectSql = "select id ,name from user_info_src";
		tableInputMeta.setSQL(selectSql);

		StepMeta tableInputStepMeta = new StepMeta(tableInputPluginId,
				"tableInput", (StepMetaInterface) tableInputMeta);

		tableInputStepMeta.setDistributes(false);

		transMeta.addStep(tableInputStepMeta);

		TableOutputMeta tableOutputMeta = new TableOutputMeta();
		tableOutputMeta.setDatabaseMeta(destDatabaseMeta);

		//设置目标表的 schema和表名
		tableOutputMeta.setSchemaName(null);
		tableOutputMeta.setTablename("user_info_dest");

		String tableOutputPluginId = registry.getPluginId(StepPluginType.class, tableOutputMeta);
		StepMeta tableOutputStep = new StepMeta(tableOutputPluginId, "tableOutput" , (StepMetaInterface) tableOutputMeta);

		//将步骤添加进去
		transMeta.addStep(tableOutputStep);

		//将步骤和上一步关联起来
		transMeta.addTransHop(new TransHopMeta(tableInputStepMeta, tableOutputStep));


		//复制记录到结果
		RowsToResultMeta rowsToResultMeta = new RowsToResultMeta();
		String rowsToResultMetaPluginId = registry.getPluginId(StepPluginType.class, rowsToResultMeta);

		//添加步骤到转换中
		StepMeta rowsToResultStep = new StepMeta(rowsToResultMetaPluginId, "rowsToResult", (StepMetaInterface)rowsToResultMeta);
		transMeta.addStep(rowsToResultStep);

		//添加hop把两个步骤关联起来
		transMeta.addTransHop(new TransHopMeta(tableInputStepMeta, rowsToResultStep));

		Trans trans = new Trans(transMeta);

		//执行转换
		trans.execute(null);

		//等待完成
		trans.waitUntilFinished();
		if (trans.getErrors() > 0) {
			System.out.println("交换出错.");
			return;
		}else{
			Result result = trans.getResult();
			List<RowMetaAndData> rows = result.getRows(); //获取数据
			for (RowMetaAndData row : rows) {
				RowMetaInterface rowMeta = row.getRowMeta(); //获取列的元数据信息
				String[] fieldNames = rowMeta.getFieldNames();
				Object[] datas = row.getData();
				for (int i = 0; i < fieldNames.length; i++) {
					System.out.println(fieldNames[i]+"="+datas[i]);
				}
			}
		}

	}

	/**
	 * hive 到 mysql 之间的交换
	 * @throws KettleException
	 */
	@Test
	public void exchangeHive2MySQL() throws KettleException{
		//源数据库连接
		String src = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
				"<connection>" +
				"<name>src</name>" +
				"<server>192.168.10.212</server>" +
				"<type>HIVE2</type>" +
				"<access>Native</access>" +
				"<database>ntzw_dev_64</database>" +
				"<port>10000</port>" +
				"<username>hadoop</username>" +
				"<password>hadoop</password>" +
				"</connection>";

		//目标数据库连接
		String dest = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
				"<connection>" +
				"<name>dest</name>" +
				"<server>192.168.10.64</server>" +
				"<type>MYSQL</type>" +
				"<access>Native</access>" +
				"<database>test</database>" +
				"<port>3306</port>" +
				"<username>root</username>" +
				"<password>root</password>" +
				"</connection>";

		DatabaseMeta srcDatabaseMeta = new DatabaseMeta(src);
		DatabaseMeta destDatabaseMeta = new DatabaseMeta(dest);

		//创建转换元信息
		TransMeta transMeta = new TransMeta();
		transMeta.setName("hive 到 mysql 之间的交换");

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
		String selectSql = "select id ,name from user_info_src";
		tableInputMeta.setSQL(selectSql);

		StepMeta tableInputStepMeta = new StepMeta(tableInputPluginId,
				"tableInput", (StepMetaInterface) tableInputMeta);
		transMeta.addStep(tableInputStepMeta);

		TableOutputMeta tableOutputMeta = new TableOutputMeta();
		tableOutputMeta.setDatabaseMeta(destDatabaseMeta);

		//设置目标表的 schema和表名
		tableOutputMeta.setSchemaName(null);
		tableOutputMeta.setTablename("user_info_dest");

		String tableOutputPluginId = registry.getPluginId(StepPluginType.class, tableOutputMeta);
		StepMeta tableOutputStep = new StepMeta(tableOutputPluginId, "tableOutput" , (StepMetaInterface) tableOutputMeta);

		//将步骤添加进去
		transMeta.addStep(tableOutputStep);

		//将步骤和上一步关联起来
		transMeta.addTransHop(new TransHopMeta(tableInputStepMeta, tableOutputStep));

		Trans trans = new Trans(transMeta);

		//执行转换
		trans.execute(null);

		//等待完成
		trans.waitUntilFinished();
		if (trans.getErrors() > 0) {
			System.out.println("交换出错.");
			return;
		}

	}

	/**
	 * hive 到 oracle 之间的交换
	 * @throws KettleException
	 */
	@Test
	public void exchangeHive2Oracle()throws KettleException{
		//源数据库连接
		String src = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
				"<connection>" +
				"<name>src</name>" +
				"<server>192.168.10.212</server>" +
				"<type>HIVE2</type>" +
				"<access>Native</access>" +
				"<database>ntzw_dev_64</database>" +
				"<port>10000</port>" +
				"<username>hadoop</username>" +
				"<password>hadoop</password>" +
				"</connection>";

		//目标数据库连接
		String dest = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
				"<connection>" +
				"<name>dest</name>" +
				"<server>192.168.1.172</server>" +
				"<type>ORACLE</type>" +
				"<access>Native</access>" +
				"<database>fableorcl</database>" +
				"<port>1521</port>" +
				"<username>fable</username>" +
				"<password>fable</password>" +
				"</connection>";

		DatabaseMeta srcDatabaseMeta = new DatabaseMeta(src);
		DatabaseMeta destDatabaseMeta = new DatabaseMeta(dest);

		//创建转换元信息
		TransMeta transMeta = new TransMeta();
		transMeta.setName("hive 到 oracle 之间的交换");

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
		String selectSql = "select id ,name from user_info_src";
		tableInputMeta.setSQL(selectSql);

		StepMeta tableInputStepMeta = new StepMeta(tableInputPluginId,
				"tableInput", (StepMetaInterface) tableInputMeta);
		transMeta.addStep(tableInputStepMeta);

		TableOutputMeta tableOutputMeta = new TableOutputMeta();
		tableOutputMeta.setDatabaseMeta(destDatabaseMeta);

		//设置目标表的 schema和表名
		tableOutputMeta.setSchemaName("TEST");
		tableOutputMeta.setTablename("user_info_dest");

		String tableOutputPluginId = registry.getPluginId(StepPluginType.class, tableOutputMeta);
		StepMeta tableOutputStep = new StepMeta(tableOutputPluginId, "tableOutput" , (StepMetaInterface) tableOutputMeta);

		//将步骤添加进去
		transMeta.addStep(tableOutputStep);

		//将步骤和上一步关联起来
		transMeta.addTransHop(new TransHopMeta(tableInputStepMeta, tableOutputStep));

		Trans trans = new Trans(transMeta);

		//执行转换
		trans.execute(null);

		//等待完成
		trans.waitUntilFinished();
		if (trans.getErrors() > 0) {
			System.out.println("交换出错.");
			return;
		}
	}

	/**
	 * oracle 到 hive 之间的交换
	 * @throws KettleException
	 */
	@Test
	public void exchangeOracle2Hive()throws KettleException{
		//源数据库连接
		String src = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
				"<connection>" +
				"<name>src</name>" +
				"<server>192.168.1.172</server>" +
				"<type>ORACLE</type>" +
				"<access>Native</access>" +
				"<database>fableorcl</database>" +
				"<port>1521</port>" +
				"<username>fable</username>" +
				"<password>fable</password>" +
				"</connection>";

		//目标数据库连接
		String dest = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
				"<connection>" +
				"<name>dest</name>" +
				"<server>192.168.10.212</server>" +
				"<type>HIVE2</type>" +
				"<access>Native</access>" +
				"<database>ntzw_dev_64</database>" +
				"<port>10000</port>" +
				"<username>hadoop</username>" +
				"<password>hadoop</password>" +
				"</connection>";

		DatabaseMeta srcDatabaseMeta = new DatabaseMeta(src);
		DatabaseMeta destDatabaseMeta = new DatabaseMeta(dest);

		//创建转换元信息
		TransMeta transMeta = new TransMeta();
		transMeta.setName("oracle 到 hive 之间的交换");

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
		String selectSql = "select ID,NAME from TEST.USER_INFO_SRC";
		tableInputMeta.setSQL(selectSql);

		StepMeta tableInputStepMeta = new StepMeta(tableInputPluginId,
				"tableInput", (StepMetaInterface) tableInputMeta);
		transMeta.addStep(tableInputStepMeta);

		//将ID,NAME列的值映射到列id,name
		SelectValuesMeta selectValuesTime = new SelectValuesMeta();
		String selectValuesPluginIdTime = registry.getPluginId(StepPluginType.class, selectValuesTime);

		selectValuesTime.allocate(2,0,0);
		String[] srcColNames = new String[]{"ID","NAME"};
		selectValuesTime.setSelectName(srcColNames);
		String[] destColNames = new String[]{"id","name"};
		selectValuesTime.setSelectRename(destColNames);

		StepMeta selectValuesStep = new StepMeta(selectValuesPluginIdTime, "selectValuesTime", (StepMetaInterface)selectValuesTime);
		transMeta.addStep(selectValuesStep);

		//将步骤和上一步关联起来
		transMeta.addTransHop(new TransHopMeta(tableInputStepMeta, selectValuesStep));


		TableOutputMeta tableOutputMeta = new TableOutputMeta();
		tableOutputMeta.setDatabaseMeta(destDatabaseMeta);

		//设置目标表的 schema和表名
		tableOutputMeta.setSchemaName(null);
		tableOutputMeta.setTablename("user_info_dest");

		String tableOutputPluginId = registry.getPluginId(StepPluginType.class, tableOutputMeta);
		StepMeta tableOutputStep = new StepMeta(tableOutputPluginId, "tableOutput" , (StepMetaInterface) tableOutputMeta);

		//将步骤添加进去
		transMeta.addStep(tableOutputStep);

		//将步骤和上一步关联起来
		transMeta.addTransHop(new TransHopMeta(selectValuesStep, tableOutputStep));

		Trans trans = new Trans(transMeta);

		//执行转换
		trans.execute(null);

		//等待完成
		trans.waitUntilFinished();
		if (trans.getErrors() > 0) {
			System.out.println("交换出错.");
			return;
		}
	}

	/**
	 * hive 到 sqlserver 之间的交换
	 * @throws KettleException
	 */
	@Test
	public void exchangeHive2MSSQL()throws KettleException{
		//源数据库连接
		String src = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
				"<connection>" +
				"<name>src</name>" +
				"<server>192.168.10.212</server>" +
				"<type>HIVE2</type>" +
				"<access>Native</access>" +
				"<database>ntzw_dev_64</database>" +
				"<port>10000</port>" +
				"<username>hadoop</username>" +
				"<password>hadoop</password>" +
				"</connection>";

		//目标数据库连接
		String dest = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
				"<connection>" +
				"<name>dest</name>" +
				"<server>192.168.230.80</server>" +
				"<type>MSSQL</type>" +
				"<access>Native</access>" +
				"<database>test</database>" +
				"<port>1433</port>" +
				"<username>sa</username>" +
				"<password>123456</password>" +
				"</connection>";

		DatabaseMeta srcDatabaseMeta = new DatabaseMeta(src);
		DatabaseMeta destDatabaseMeta = new DatabaseMeta(dest);

		//创建转换元信息
		TransMeta transMeta = new TransMeta();
		transMeta.setName("hive 到 sqlserver 之间的交换");

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
		String selectSql = "select id ,name from user_info_src";
		tableInputMeta.setSQL(selectSql);

		StepMeta tableInputStepMeta = new StepMeta(tableInputPluginId,
				"tableInput", (StepMetaInterface) tableInputMeta);
		transMeta.addStep(tableInputStepMeta);

		TableOutputMeta tableOutputMeta = new TableOutputMeta();
		tableOutputMeta.setDatabaseMeta(destDatabaseMeta);

		//设置目标表的 schema和表名
		tableOutputMeta.setSchemaName(null);
		tableOutputMeta.setTablename("user_info_dest");

		String tableOutputPluginId = registry.getPluginId(StepPluginType.class, tableOutputMeta);
		StepMeta tableOutputStep = new StepMeta(tableOutputPluginId, "tableOutput" , (StepMetaInterface) tableOutputMeta);

		//将步骤添加进去
		transMeta.addStep(tableOutputStep);

		//将步骤和上一步关联起来
		transMeta.addTransHop(new TransHopMeta(tableInputStepMeta, tableOutputStep));

		Trans trans = new Trans(transMeta);

		//执行转换
		trans.execute(null);

		//等待完成
		trans.waitUntilFinished();
		if (trans.getErrors() > 0) {
			System.out.println("交换出错.");
			return;
		}
	}

	/**
	 * sqlserver 到 hive 之间的交换
	 * @throws KettleException
	 */
	@Test
	public void exchangeMSSQL2Hive()throws KettleException{
		//源数据库连接
		String src = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
				"<connection>" +
				"<name>src</name>" +
				"<server>192.168.230.80</server>" +
				"<type>MSSQL</type>" +
				"<access>Native</access>" +
				"<database>test</database>" +
				"<port>1433</port>" +
				"<username>sa</username>" +
				"<password>123456</password>" +
				"</connection>";

		//目标数据库连接
		String dest = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
				"<connection>" +
				"<name>dest</name>" +
				"<server>192.168.10.212</server>" +
				"<type>Hive</type>" +
				"<access>Native</access>" +
				"<database>ntzw_dev_64</database>" +
				"<port>10000</port>" +
				"<username>hadoop</username>" +
				"<password>hadoop</password>" +
				"</connection>";

		DatabaseMeta srcDatabaseMeta = new DatabaseMeta(src);
		DatabaseMeta destDatabaseMeta = new DatabaseMeta(dest);

		//创建转换元信息
		TransMeta transMeta = new TransMeta();
		transMeta.setName("sqlserver 到 hive 之间的交换");

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
		String selectSql = "select id ,name from user_info_src";
		tableInputMeta.setSQL(selectSql);

		StepMeta tableInputStepMeta = new StepMeta(tableInputPluginId,
				"tableInput", (StepMetaInterface) tableInputMeta);
		transMeta.addStep(tableInputStepMeta);

		TableOutputMeta tableOutputMeta = new TableOutputMeta();
		tableOutputMeta.setDatabaseMeta(destDatabaseMeta);

		//设置目标表的 schema和表名
		tableOutputMeta.setSchemaName(null);
		tableOutputMeta.setTablename("user_info_dest");

		String tableOutputPluginId = registry.getPluginId(StepPluginType.class, tableOutputMeta);
		StepMeta tableOutputStep = new StepMeta(tableOutputPluginId, "tableOutput" , (StepMetaInterface) tableOutputMeta);

		//将步骤添加进去
		transMeta.addStep(tableOutputStep);

		//将步骤和上一步关联起来
		transMeta.addTransHop(new TransHopMeta(tableInputStepMeta, tableOutputStep));

		Trans trans = new Trans(transMeta);

		//执行转换
		trans.execute(null);

		//等待完成
		trans.waitUntilFinished();
		if (trans.getErrors() > 0) {
			System.out.println("交换出错.");
			return;
		}
	}
}
