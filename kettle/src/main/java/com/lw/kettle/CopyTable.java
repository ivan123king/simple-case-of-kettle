//package com.lw.kettle;
//
//import java.text.MessageFormat;
//import java.util.List;
//
//import com.lw.bean.DatabaseConn;
//import com.lw.bean.ExtractBean;
//import org.apache.commons.lang3.ArrayUtils;
//import org.apache.commons.lang3.StringUtils;
//import org.junit.Before;
//import org.junit.Test;
//import org.pentaho.di.core.KettleEnvironment;
//import org.pentaho.di.core.Result;
//import org.pentaho.di.core.RowMetaAndData;
//import org.pentaho.di.core.database.DatabaseMeta;
//import org.pentaho.di.core.exception.KettleException;
//import org.pentaho.di.core.exception.KettleXMLException;
//import org.pentaho.di.core.logging.LogLevel;
//import org.pentaho.di.core.plugins.PluginRegistry;
//import org.pentaho.di.core.plugins.StepPluginType;
//import org.pentaho.di.core.row.RowMetaInterface;
//import org.pentaho.di.core.util.EnvUtil;
//import org.pentaho.di.trans.Trans;
//import org.pentaho.di.trans.TransAdapter;
//import org.pentaho.di.trans.TransHopMeta;
//import org.pentaho.di.trans.TransMeta;
//import org.pentaho.di.trans.step.StepMeta;
//import org.pentaho.di.trans.step.StepMetaInterface;
//import org.pentaho.di.trans.steps.jsonoutput.JsonOutputField;
//import org.pentaho.di.trans.steps.jsonoutput.JsonOutputMeta;
//import org.pentaho.di.trans.steps.replacestring.ReplaceStringMeta;
//import org.pentaho.di.trans.steps.rowstoresult.RowsToResultMeta;
//import org.pentaho.di.trans.steps.selectvalues.SelectValuesMeta;
//import org.pentaho.di.trans.steps.tableinput.TableInputMeta;
//import org.pentaho.di.trans.steps.tableoutput.TableOutputMeta;
//
//public class CopyTable {
//
//	ExtractBean extractBean = new ExtractBean();
//
//	@Before
//	public void before() {
//		try {
//			// 初始化Kettle环境
//			KettleEnvironment.init();
//			EnvUtil.environmentInit();
//
//			initMySQLTest();
//		} catch (KettleException e) {
//			e.printStackTrace();
//		}
//
//
//	}
//
//	private void initMySQLTest(){
//		// 初始化数据源表和目标表数据
//				ExtractBean extractBean = new ExtractBean();
//				DatabaseConn srcDB = new DatabaseConn();
//				srcDB.setDatabase("test");
//				srcDB.setServer("127.0.0.1");
//				srcDB.setPort("3306");
//				srcDB.setUsername("root");
//				srcDB.setPassword("root");
//				srcDB.setType("MySQL");
//				extractBean.setSrcDB(srcDB);
//				extractBean
//						.setSrcFields(new String[] { "name", "age", "mail", "phone" });
//				extractBean.setSrcPk(new String[] { "id" });
//				extractBean.setSrcTable(new String[] { "etl_src_table" });
//
//				// 数据转换
//				FieldTransfer[] fieldTransfers = new FieldTransfer[2];
//				FieldTransfer nameTransfer = new FieldTransfer();
//				nameTransfer.setField("name");
//				nameTransfer.setSrc("king");
//				nameTransfer.setTarget("lw");
//				fieldTransfers[0] = nameTransfer;
//				FieldTransfer mailTransfer = new FieldTransfer();
//				mailTransfer.setField("mail");
//				mailTransfer.setSrc("^lw.*@.*//.com$");
//				mailTransfer.setTarget("lw***.com");
//				mailTransfer.setRegEx(true);
//				fieldTransfers[1] = mailTransfer;
//				extractBean.setFieldTransfers(fieldTransfers);
//
//				DatabaseConn destDB = new DatabaseConn();
//				destDB.setDatabase("test");
//				destDB.setServer("127.0.0.1");
//				destDB.setPort("3306");
//				destDB.setUsername("root");
//				destDB.setPassword("root");
//				destDB.setType("MySQL");
//				extractBean.setDestDB(destDB);
//				extractBean
//						.setDestFields(new String[] { "name", "age", "mail", "phone" });
//				extractBean.setDestPk(new String[] { "id" });
//				extractBean.setDestTable("etl_dest_table");
//				this.extractBean = extractBean;
//	}
//
//	/**
//     * 数据库连接信息,适用于DatabaseMeta其中 一个构造器DatabaseMeta(String xml)
//     */
//	protected String databasesXML =
//		"<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
//			+ "<connection>"
//			+ "<name>{0}</name>"
//			+ "<server>{1}</server>"
//			+ "<type>{2}</type>"
//			+ "<access>{3}</access>"
//			+ "<database>{4}</database>"
//			+ "<port>{5}</port>"
//			+ "<username>{6}</username>"
//			+ "<password>{7}</password>"
//			+ "</connection>";
//
//	/**
//	 * 5. 将源表数据拷贝到目标表中
//	 * @throws KettleXMLException
//	 */
//	@Test
//	public void copySrc2Dest() throws KettleXMLException{
//		ExtractBean extractBean = CopyTable.this.extractBean;
//
//		TransMeta transMeta = new TransMeta();
//		transMeta.setName("全量抽取数据");
//
//		DatabaseConn srcDB = extractBean.getSrcDB();
//		final String srcDBName = "srcDB";
//		DatabaseMeta dbMeta = new DatabaseMeta(srcDBName, srcDB.getType(),
//				srcDB.getAccess(), srcDB.getServer(), srcDB.getDatabase(),
//				srcDB.getPort(), srcDB.getUsername(), srcDB.getPassword());
//
//		dbMeta.setConnectSQL(MessageFormat.format("set names ''{0}'';",
//				new Object[] { "utf8" }));
//
//		transMeta.addDatabase(dbMeta);
//
//
//		PluginRegistry registry = PluginRegistry.getInstance();
//		TableInputMeta tableInputMeta = new TableInputMeta();
//		String tableInputPluginId = registry.getPluginId(StepPluginType.class,
//				tableInputMeta);
//		/*
//		 *  1. 给表输入添加一个DatabaseMeta连接数据库
//		 */
//		tableInputMeta.setDatabaseMeta(transMeta.findDatabase(srcDBName));
//		/*
//		 * 2.  构造查询SQL
//		 */
//		String selectSql = "select {0} from {1}";
//		selectSql = MessageFormat.format(
//				selectSql,
//				new Object[] {
//						StringUtils.join(extractBean.getSrcFields(), ","),
//						extractBean.getSrcTable()[0] });
//		tableInputMeta.setSQL(selectSql);
//
//		// 打印查询SQL
//		System.out.println(selectSql);
//
//		/*
//		 * 4. 将TableInputMeta添加到转换中
//		 */
//		StepMeta tableInputStepMeta = new StepMeta(tableInputPluginId,
//				"tableInput", (StepMetaInterface) tableInputMeta);
//		transMeta.addStep(tableInputStepMeta);
//
//		/*
//		 * 10. 获取目标库信息
//		 */
//		DatabaseConn destDB = extractBean.getDestDB();
//		final String destDBName = "destDB";
//		DatabaseMeta destDbMeta = new DatabaseMeta(destDBName, destDB.getType(),
//				destDB.getAccess(), destDB.getServer(), destDB.getDatabase(),
//				destDB.getPort(), destDB.getUsername(), destDB.getPassword());
//
//		/*
//		 * 12.输出到表中
//		 */
//		TableOutputMeta tableOutputMeta = new TableOutputMeta();
//		tableOutputMeta.setDatabaseMeta(destDbMeta);
//
//		//设置目标表的 schema和表名
//		tableOutputMeta.setSchemaName(null);
//		tableOutputMeta.setTablename(extractBean.getDestTable());
//
//		//指定目标表数据库字段
//		tableOutputMeta.setSpecifyFields(true);
//		/**
//		 * 在此处如果是oracle中含有大字段的有一个问题：
//		 * ORA-24816: 在实际的 LONG 或 LOB 列之后提供了扩展的非 LONG 绑定数据
//		解决方法：在实际的 LONG 或 LOB 列之后提供了扩展的非 LONG 绑定数据错误，
//		这个错误是因为在绑定参数时把数据库中字段类型为LONG的字段放置在其他字段前设置了，
//		只要将类型为Lob的字段绑定参数时放在最后设置即可解决。
//
//		即设置  destFields为：  long ,clob,blob这样的顺序，long类型等不能放在clob后面
//		 */
//		tableOutputMeta.setFieldStream(extractBean.getDestFields());
//		tableOutputMeta.setFieldDatabase(extractBean.getDestFields());
//
//		String tableOutputPluginId = registry.getPluginId(StepPluginType.class, tableOutputMeta);
//		StepMeta tableOutputStep = new StepMeta(tableOutputPluginId, "tableOutput" , (StepMetaInterface) tableOutputMeta);
//
//		//将步骤添加进去
//		transMeta.addStep(tableOutputStep);
//
//		//将步骤和上一步关联起来
//		transMeta.addTransHop(new TransHopMeta(tableInputStepMeta, tableOutputStep));
//
//		Trans trans = new Trans(transMeta);
//		trans.setMonitored(true);
//		trans.setInitializing(true);
//		trans.setPreparing(true);
//		trans.setLogLevel(LogLevel.BASIC);
//		trans.setRunning(true);
//		trans.setSafeModeEnabled(true);
//
//		trans.addTransListener(new TransAdapter() {
//			@Override
//			public void transFinished(Trans trans) {
//				System.out.println("转换执行完成");
//			}
//		});
//
//		try {
//			// trans.startThreads();
//			trans.execute(null);
//		} catch (KettleException e) {
//			e.printStackTrace();
//		}
//		trans.waitUntilFinished();
//		if (trans.getErrors() > 0) {
//			System.out.println("抽取数据出错.");
//			return;
//		}
//
//		System.out.println("抽取加载数据完成");
//
//	}
//
//	/**
//	 * 1. 源表数据变成json字段输出
//	 * @throws KettleXMLException
//	 */
//	@Test
//	public void getTableData2Json() throws KettleXMLException {
//
//		ExtractBean extractBean = CopyTable.this.extractBean;
//
//		TransMeta transMeta = new TransMeta();
//		transMeta.setName("全量抽取数据");
//
//		DatabaseConn srcDB = extractBean.getSrcDB();
//		final String srcDBName = "srcDB";
//		DatabaseMeta dbMeta = new DatabaseMeta(srcDBName, srcDB.getType(),
//				srcDB.getAccess(), srcDB.getServer(), srcDB.getDatabase(),
//				srcDB.getPort(), srcDB.getUsername(), srcDB.getPassword());
//
//		//添加转换的数据库连接
////        String xml = MessageFormat.format(databasesXML, new Object[] {
////        		srcDBName,
////        		srcDB.getServer(),
////        		"MySQL",
////        		"Native",
////        		srcDB.getDatabase(),
////        		srcDB.getPort(),
////        		srcDB.getUsername(),
////        		srcDB.getPassword()
////        });
////		DatabaseMeta dbMeta = new DatabaseMeta(xml);
//
//		dbMeta.setConnectSQL(MessageFormat.format("set names ''{0}'';",
//				new Object[] { "utf8" }));
//
//		transMeta.addDatabase(dbMeta);
//
//		PluginRegistry registry = PluginRegistry.getInstance();
//		TableInputMeta tableInputMeta = new TableInputMeta();
//		String tableInputPluginId = registry.getPluginId(StepPluginType.class,
//				tableInputMeta);
//		// 给表输入添加一个DatabaseMeta连接数据库
//		tableInputMeta.setDatabaseMeta(transMeta.findDatabase(srcDBName));
//		// 构造查询SQL
//		String selectSql = "select {0} from {1}";
//		selectSql = MessageFormat.format(
//				selectSql,
//				new Object[] {
//						StringUtils.join(extractBean.getSrcFields(), ","),
//						extractBean.getSrcTable()[0] });
//		tableInputMeta.setSQL(selectSql);
//
//		// 打印查询SQL
//		System.out.println(selectSql);
//
//		// 替换SQL语句中变量
//		tableInputMeta.setVariableReplacementActive(true);
//
//		// 将TableInputMeta添加到转换中
//		StepMeta tableInputStepMeta = new StepMeta(tableInputPluginId,
//				"tableInput", (StepMetaInterface) tableInputMeta);
//		transMeta.addStep(tableInputStepMeta);
//
//		// 字段选择
//		SelectValuesMeta selectValuesMeta = new SelectValuesMeta();
//		String selectValuesPluginId = registry.getPluginId(
//				StepPluginType.class, selectValuesMeta);
//		selectValuesMeta.allocate(extractBean.getSrcFields().length, 0, 0);
//		selectValuesMeta.setSelectName(extractBean.getSrcFields());
//
//		StepMeta selectValuesStepMeta = new StepMeta(selectValuesPluginId,
//				"selectValues", (StepMetaInterface) selectValuesMeta);
//		transMeta.addStep(selectValuesStepMeta);
//
//		// 添加hop把两个步骤关联起来
//		transMeta.addTransHop(new TransHopMeta(tableInputStepMeta,
//				selectValuesStepMeta));
//
//		// 字符串替换
//		ReplaceStringMeta replaceStringMeta = new ReplaceStringMeta();
//		String replaceStringPluginId = registry.getPluginId(
//				StepPluginType.class, replaceStringMeta);
//		int fieldTransferLength = extractBean.getFieldTransfers().length;
//		replaceStringMeta.allocate(fieldTransferLength);
//		for (int i = 0; i < fieldTransferLength; i++) {
//			FieldTransfer fieldTransfer = extractBean.getFieldTransfers()[i];
//			replaceStringMeta.getFieldInStream()[i] = fieldTransfer.getField();
//			replaceStringMeta.getReplaceString()[i] = fieldTransfer.getSrc();
//			replaceStringMeta.getReplaceByString()[i] = fieldTransfer
//					.getTarget();
//			//是否使用正则表达式
//			replaceStringMeta.getUseRegEx()[i] = fieldTransfer.isRegEx() ? 1
//					: 0;
//		}
//
//		if (extractBean.getFieldTransfers() == null || fieldTransferLength <= 0) {
//			replaceStringMeta.allocate(0);
//		}
//
//		// 添加步骤到转换中
//		StepMeta replaceStringStepMeta = new StepMeta(replaceStringPluginId,
//				"replaceString", (StepMetaInterface) replaceStringMeta);
//		transMeta.addStep(replaceStringStepMeta);
//		// 添加hop把两个步骤关联起来
//		transMeta.addTransHop(new TransHopMeta(selectValuesStepMeta,
//				replaceStringStepMeta));
//
//		// json输出
//		JsonOutputMeta jsonOutput = new JsonOutputMeta();
//		String jsonOutputPluginId = registry.getPluginId(StepPluginType.class,
//				jsonOutput);
//		jsonOutput.setOperationType(JsonOutputMeta.OPERATION_TYPE_OUTPUT_VALUE);
//		jsonOutput.setJsonBloc("data");
//		jsonOutput.setOutputValue("rows");
//
//		int srcFieldLength = extractBean.getSrcFields().length;
//		JsonOutputField[] outputFields = new JsonOutputField[srcFieldLength];
//		for (int i = 0; i < srcFieldLength; i++) {
//			JsonOutputField field = new JsonOutputField();
//			field.setFieldName(extractBean.getSrcFields()[i]);
//			field.setElementName(extractBean.getSrcFields()[i]);
//
//			outputFields[i] = field;
//		}
//		jsonOutput.setOutputFields(outputFields);
//
//		// 添加步骤到转换中
//		StepMeta jsonOutputStepMeta = new StepMeta(jsonOutputPluginId,
//				"jsonOutput", (StepMetaInterface) jsonOutput);
//		transMeta.addStep(jsonOutputStepMeta);
//		// 添加hop把两个步骤关联起来
//		transMeta.addTransHop(new TransHopMeta(replaceStringStepMeta,
//				jsonOutputStepMeta));
//
//		// 复制记录到结果
//		RowsToResultMeta rowsToResultMeta = new RowsToResultMeta();
//		String rowsToResultMetaPluginId = registry.getPluginId(
//				StepPluginType.class, rowsToResultMeta);
//		// 添加步骤到转换中
//		StepMeta rowsToResultStepMeta = new StepMeta(rowsToResultMetaPluginId,
//				"rowsToResult", (StepMetaInterface) rowsToResultMeta);
//		transMeta.addStep(rowsToResultStepMeta);
//		// hop把两个步骤关联起来
//		transMeta.addTransHop(new TransHopMeta(jsonOutputStepMeta,
//				rowsToResultStepMeta));
//
//		//这个的作用是用来，设置select这个SQL中分页的
//		transMeta.setVariable("VAR_FROM", "0");
//		transMeta.setVariable("VAR_TO", "20");
//
//		Trans trans = new Trans(transMeta);
//		trans.setMonitored(true);
//		trans.setInitializing(true);
//		trans.setPreparing(true);
//		trans.setLogLevel(LogLevel.BASIC);
//		trans.setRunning(true);
//		trans.setSafeModeEnabled(true);
//
//		trans.addTransListener(new TransAdapter() {
//			@Override
//			public void transFinished(Trans trans) {
//				System.out.println("转换执行完成");
//			}
//		});
//
//		try {
//			// trans.startThreads();
//			trans.execute(null);
//		} catch (KettleException e) {
//			e.printStackTrace();
//		}
//		trans.waitUntilFinished();
//		if (trans.getErrors() > 0) {
//			System.out.println("抽取数据出错.");
//			return;
//		}
//
//		Result result = trans.getResult();
//		List<RowMetaAndData> list = result.getRows();
//		String fieldNames = "";
//		if (list != null && list.size() > 0) {
//			RowMetaAndData row = list.get(0);
//			RowMetaInterface rowMeta = row.getRowMeta();
//			Object[] srcFields = ArrayUtils.subarray(rowMeta.getFieldNames(),
//					0, this.extractBean.getSrcFields().length);
//			fieldNames = StringUtils.join(srcFields, ",");
//
//			Object[] cols = row.getData();
//			// 遍历方式获取
//			String json = "{}";
//			for (int i = 0; i < cols.length; i++) {
//				if (cols[i] != null) {
//					json = cols[i].toString();
//					break;
//				}
//			}
//			System.out.println("抽取数据为：" + json);
//		}
//
//	}
//
//	/**
//	 * 2.建立临时表和表触发器
//	 * 分为三种，oracle,mysql,sqlserver数据库
//	 */
//	@Test
//	public void createTriggerAndTmpTable() {
//		/*
//		 *1.获取模板
//		 */
//
//		/*
//		 * 2. 生成建立临时表语句
//		 */
//
//		/*
//		 * 3. 生成建立触发器语句
//		 */
//
//		/*
//		 * 4. 执行
//		 */
//	}
//
//	/**
//	 * 3.触发器增量交换
//	 */
//	@Test
//	public void changeWithTrigger() {
//
//	}
//
//	/**
//	 * 4.时间戳交换
//	 */
//	@Test
//	public void changeWithTime() {
//
//	}
//}
