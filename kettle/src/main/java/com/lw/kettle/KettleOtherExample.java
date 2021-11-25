//package com.lw.kettle;
//
//import com.lw.bean.ReplaceStringBean;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.commons.lang.ArrayUtils;
//import org.junit.Before;
//import org.junit.Test;
//import org.pentaho.di.core.KettleEnvironment;
//import org.pentaho.di.core.Result;
//import org.pentaho.di.core.RowMetaAndData;
//import org.pentaho.di.core.RowSet;
//import org.pentaho.di.core.database.DatabaseMeta;
//import org.pentaho.di.core.exception.KettleException;
//import org.pentaho.di.core.plugins.PluginRegistry;
//import org.pentaho.di.core.plugins.StepPluginType;
//import org.pentaho.di.core.row.RowMetaInterface;
//import org.pentaho.di.core.util.EnvUtil;
//import org.pentaho.di.trans.Trans;
//import org.pentaho.di.trans.TransHopMeta;
//import org.pentaho.di.trans.TransMeta;
//import org.pentaho.di.trans.step.*;
//import org.pentaho.di.trans.steps.insertupdate.InsertUpdateMeta;
//import org.pentaho.di.trans.steps.jsonoutput.JsonOutputField;
//import org.pentaho.di.trans.steps.jsonoutput.JsonOutputMeta;
//import org.pentaho.di.trans.steps.replacestring.ReplaceStringMeta;
//import org.pentaho.di.trans.steps.rowstoresult.RowsToResultMeta;
//import org.pentaho.di.trans.steps.setvariable.SetVariableMeta;
//import org.pentaho.di.trans.steps.tableinput.TableInputMeta;
//import org.pentaho.di.trans.steps.tableoutput.TableOutputMeta;
//
//import java.util.ArrayList;
//import java.util.List;
//
///**
// * @author lw
// * @date 2021/8/23 0023
// * @description  kettle的其他应用
// */
//@Slf4j
//public class KettleOtherExample {
//
//    @Before
//    public void before() {
//        try {
//            // 初始化Kettle环境
//            KettleEnvironment.init();
//            EnvUtil.environmentInit();
//        } catch (KettleException e) {
//            e.printStackTrace();
//        }
//    }
//
//    /**
//     * mysql到mysql之间的交换
//     * @throws KettleException
//     */
//    @Test
//    public void exchangeMySQL2MySQL() throws KettleException{
//        //源数据库连接
//        String src = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
//                "<connection>" +
//                "<name>src</name>" +
//                "<server>192.168.10.64</server>" +
//                "<type>MYSQL</type>" +
//                "<access>Native</access>" +
//                "<database>test</database>" +
//                "<port>3306</port>" +
//                "<username>root</username>" +
//                "<password>root</password>" +
//                "</connection>";
//
//        //目标数据库连接
//        String dest = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
//                "<connection>" +
//                "<name>dest</name>" +
//                "<server>192.168.10.64</server>" +
//                "<type>MYSQL</type>" +
//                "<access>Native</access>" +
//                "<database>test</database>" +
//                "<port>3306</port>" +
//                "<username>root</username>" +
//                "<password>root</password>" +
//                "</connection>";
//
//        DatabaseMeta srcDatabaseMeta = new DatabaseMeta(src);
//        DatabaseMeta destDatabaseMeta = new DatabaseMeta(dest);
//
//        //创建转换元信息
//        TransMeta transMeta = new TransMeta();
//        transMeta.setName("mysql到mysql之间的交换");
//
//        //设置源和目标
//        transMeta.addDatabase(srcDatabaseMeta);
//        transMeta.addDatabase(destDatabaseMeta);
//
//		/*
//		 * 创建  表输入->插入/更新
//		 * 同时将两个步骤连接起来
//		 */
//        PluginRegistry registry = PluginRegistry.getInstance();
//        TableInputMeta tableInputMeta = new TableInputMeta();
//        String tableInputPluginId = registry.getPluginId(StepPluginType.class,
//                tableInputMeta);
//
//        tableInputMeta.setDatabaseMeta(srcDatabaseMeta);
//        //设置查询条件
//        String selectSql = "select id ,name from user_info_src";
//        tableInputMeta.setSQL(selectSql);
//
//        StepMeta tableInputStep = new StepMeta(tableInputPluginId,
//                "tableInput", (StepMetaInterface) tableInputMeta);
//        transMeta.addStep(tableInputStep);
//
//
//        InsertUpdateMeta insertUpdateMeta = new InsertUpdateMeta();
//        insertUpdateMeta.setDatabaseMeta(destDatabaseMeta);
//
//        //设置目标表的 schema和表名
//        insertUpdateMeta.setSchemaName(null);
//        insertUpdateMeta.setTableName("user_info_dest");
//
//        //设置查询关键字
//        String[] destPk = new String[]{"id_d"};
//        String[] srcPk = new String[]{"id"};
//        insertUpdateMeta.setKeyLookup(destPk);
//        insertUpdateMeta.setKeyStream(srcPk);//流里的字段对应输入
//        insertUpdateMeta.setKeyStream2(destPk);
//
//        String[] condition = new String[]{"="};
//        insertUpdateMeta.setKeyCondition(condition);//对比条件
//
//
//        //设置更新的字段
//        String[] destFields = new String[]{"id_d","name_d"};
//        String[] srcFields = new String[]{"id","name"};
//        insertUpdateMeta.setUpdateStream(srcFields);
//        insertUpdateMeta.setUpdateLookup(destFields);
//
//        //设置是否更新
//        Boolean[] updateFlags = new Boolean[]{true,true};
//        insertUpdateMeta.setUpdate(updateFlags);
//
//        String insertUpdatePluginId = registry.getPluginId(StepPluginType.class, insertUpdateMeta);
//        StepMeta insertUpdateStep = new StepMeta(insertUpdatePluginId, "insertUpdate" , (StepMetaInterface) insertUpdateMeta);
//
//        //将步骤添加进去
//        transMeta.addStep(insertUpdateStep);
//
//        //将步骤和上一步关联起来
//        transMeta.addTransHop(new TransHopMeta(tableInputStep, insertUpdateStep));
//
//        Trans trans = new Trans(transMeta);
//
//        //执行转换
//        trans.execute(null);
//
//        //等待完成
//        trans.waitUntilFinished();
//        if (trans.getErrors() > 0) {
//            System.out.println("交换出错.");
//            return;
//        }
//    }
//
//    /**
//     * 初始化替换数据
//     * @return
//     */
//    private List<ReplaceStringBean> initReplaceData(){
//        List<ReplaceStringBean> list = new ArrayList<>();
//
//        /*
//        设置两条替换
//        将lw替换为king
//        将刘伟替换为刘大壮的山
//         */
//        ReplaceStringBean bean = new ReplaceStringBean();
//        bean.setBean("lw","king");
//        list.add(bean);
//
//        bean = new ReplaceStringBean();
//        bean.setBean("刘伟","刘大壮的山");
//        list.add(bean);
//
//        return list;
//    }
//
//
//    /**
//     * 获取执行步骤的信息
//     * @throws KettleException
//     */
//    @Test
//    public void getStepInfo() throws KettleException{
//        //源数据库连接
//        String mysqL_src = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
//                "<connection>" +
//                "<name>mysqL_src</name>" +
//                "<server>192.168.10.64</server>" +
//                "<type>MYSQL</type>" +
//                "<access>Native</access>" +
//                "<database>test</database>" +
//                "<port>3306</port>" +
//                "<username>root</username>" +
//                "<password>root</password>" +
//                "</connection>";
//
//        DatabaseMeta srcDatabaseMeta = new DatabaseMeta(mysqL_src);
//
//        //创建转换元信息
//        TransMeta transMeta = new TransMeta();
//        transMeta.setName("获取查询为json");
//
//        //设置源和目标
//        transMeta.addDatabase(srcDatabaseMeta);
//
//		/*
//		 * 创建  表输入->json输出
//		 * 同时将两个步骤连接起来
//		 */
//        PluginRegistry registry = PluginRegistry.getInstance();
//        TableInputMeta tableInputMeta = new TableInputMeta();
//        String tableInputPluginId = registry.getPluginId(StepPluginType.class,
//                tableInputMeta);
//
//        tableInputMeta.setDatabaseMeta(srcDatabaseMeta);
//        //设置查询条件
//        String selectSql = "select id,name from user_info_src";
//        tableInputMeta.setSQL(selectSql);
//
//        StepMeta tableInputStep = new StepMeta(tableInputPluginId,
//                "tableInput", (StepMetaInterface) tableInputMeta);
//        transMeta.addStep(tableInputStep);
//
//        //Json结果输出
//        JsonOutputMeta jsonOutputMeta = new JsonOutputMeta();
//
//        //此处是输出到值
//        jsonOutputMeta.setOperationType(JsonOutputMeta.OPERATION_TYPE_OUTPUT_VALUE);//这里有三种，写文件，输出值，写文件同时输出值
//        jsonOutputMeta.setJsonBloc("data");
//
//        jsonOutputMeta.setOutputValue("rows"); //这个是OPERATION_TYPE_OUTPUT_VALUE，OPERATION_TYPE_BOTH 才有用
//
////        jsonOutputMeta.setExtension("txt");
////        jsonOutputMeta.setFileName("F:\\tmp\\json3");
//
//        //设置列
//        String[] colNames = new String[]{"id","name"};
//        JsonOutputField[] outputFields = new JsonOutputField[colNames.length];
//        for(int i = 0; i < colNames.length; i++) {
//            JsonOutputField field = new JsonOutputField();
//            String fieldName = colNames[i];
//            field.setFieldName(fieldName);
//            field.setElementName(fieldName);
//            outputFields[i] = field;
//        }
//        jsonOutputMeta.setOutputFields(outputFields);
//
//
//        //添加步骤到转换中
//        String jsonOutputPluginId = registry.getPluginId(StepPluginType.class, jsonOutputMeta);
//        StepMeta jsonOutputStep = new StepMeta(jsonOutputPluginId, "jsonOutput", (StepMetaInterface)jsonOutputMeta);
//        transMeta.addStep(jsonOutputStep);
//
//        //添加hop把两个步骤关联起来
//        transMeta.addTransHop(new TransHopMeta(tableInputStep, jsonOutputStep));
//
//        Trans trans = new Trans(transMeta);
//
//        //执行转换
//        trans.execute(null);
//
//        //等待完成
//        trans.waitUntilFinished();
//        if (trans.getErrors() > 0) {
//            System.out.println("交换出错.");
//            return;
//        }
//
//
//        List<StepMetaDataCombi> steps = trans.getSteps();
//        for (StepMetaDataCombi step : steps) {
//            long i = 0; //输入数据量
//            long o = 0;  //输出数据量
//            long r = 0; //读取的数据数量
//            long w = 0; //写入的数据量
//            long u = 0; //更新的数据量
//            long e = 0; //错误的数据量
//            long inputSize = 0; //输入量
//            long outputSize = 0;//输出量
//
//            StepInterface si = step.step;
//
//            i += si.getLinesInput();
//            o += si.getLinesOutput();
//            r += si.getLinesRead();
//            w += si.getLinesWritten();
//            u += si.getLinesUpdated();
//            e += si.getErrors();
//            inputSize += si.rowsetInputSize();
//            outputSize += si.rowsetOutputSize();
//
//            String outputInfo = String.format(step.stepname+"(I=%d,O=%d,R=%d,W=%d,U=%d,E=%d,is=%d,os=%d)",i,o,r,w,u,e,inputSize,outputSize);
//            System.out.println(outputInfo);
//        }
//    }
//
//    /**
//     * 获取查询为json
//     * @throws KettleException
//     */
//    @Test
//    public void getJsonFromSQL() throws KettleException{
//        //源数据库连接
//        String mysqL_src = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
//                "<connection>" +
//                "<name>mysqL_src</name>" +
//                "<server>192.168.10.64</server>" +
//                "<type>MYSQL</type>" +
//                "<access>Native</access>" +
//                "<database>test</database>" +
//                "<port>3306</port>" +
//                "<username>root</username>" +
//                "<password>root</password>" +
//                "</connection>";
//
//        DatabaseMeta srcDatabaseMeta = new DatabaseMeta(mysqL_src);
//
//        //创建转换元信息
//        TransMeta transMeta = new TransMeta();
//        transMeta.setName("获取查询为json");
//
//        //设置源和目标
//        transMeta.addDatabase(srcDatabaseMeta);
//
//		/*
//		 * 创建  表输入->json输出
//		 * 同时将两个步骤连接起来
//		 */
//        PluginRegistry registry = PluginRegistry.getInstance();
//        TableInputMeta tableInputMeta = new TableInputMeta();
//        String tableInputPluginId = registry.getPluginId(StepPluginType.class,
//                tableInputMeta);
//
//        tableInputMeta.setDatabaseMeta(srcDatabaseMeta);
//        //设置查询条件
//        String selectSql = "select id,name from user_info_src";
//        tableInputMeta.setSQL(selectSql);
//
//        StepMeta tableInputStep = new StepMeta(tableInputPluginId,
//                "tableInput", (StepMetaInterface) tableInputMeta);
//        transMeta.addStep(tableInputStep);
//
//        //Json结果输出
//        JsonOutputMeta jsonOutputMeta = new JsonOutputMeta();
//
//        //此处是输出到值
//        jsonOutputMeta.setOperationType(JsonOutputMeta.OPERATION_TYPE_OUTPUT_VALUE);//这里有三种，写文件，输出值，写文件同时输出值
//        jsonOutputMeta.setJsonBloc("data");
//
//        jsonOutputMeta.setOutputValue("rows"); //这个是OPERATION_TYPE_OUTPUT_VALUE，OPERATION_TYPE_BOTH 才有用
//
////        jsonOutputMeta.setExtension("txt");
////        jsonOutputMeta.setFileName("F:\\tmp\\json3");
//
//        //设置列
//        String[] colNames = new String[]{"id","name"};
//        JsonOutputField[] outputFields = new JsonOutputField[colNames.length];
//        for(int i = 0; i < colNames.length; i++) {
//            JsonOutputField field = new JsonOutputField();
//            String fieldName = colNames[i];
//            field.setFieldName(fieldName);
//            field.setElementName(fieldName);
//            outputFields[i] = field;
//        }
//        jsonOutputMeta.setOutputFields(outputFields);
//
//
//        //添加步骤到转换中
//        String jsonOutputPluginId = registry.getPluginId(StepPluginType.class, jsonOutputMeta);
//        StepMeta jsonOutputStep = new StepMeta(jsonOutputPluginId, "jsonOutput", (StepMetaInterface)jsonOutputMeta);
//        transMeta.addStep(jsonOutputStep);
//
//        //添加hop把两个步骤关联起来
//        transMeta.addTransHop(new TransHopMeta(tableInputStep, jsonOutputStep));
//
//        Trans trans = new Trans(transMeta);
//
//        //执行转换
//        trans.execute(null);
//
//        //等待完成
//        trans.waitUntilFinished();
//        if (trans.getErrors() > 0) {
//            System.out.println("交换出错.");
//            return;
//        }
//    }
//
//    /**
//     * 获取查询结果
//     * @throws KettleException
//     */
//    @Test
//    public void getResultFromTrans() throws KettleException{
//        //源数据库连接
//        String mysqL_src = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
//                "<connection>" +
//                "<name>mysqL_src</name>" +
//                "<server>192.168.10.64</server>" +
//                "<type>MYSQL</type>" +
//                "<access>Native</access>" +
//                "<database>test</database>" +
//                "<port>3306</port>" +
//                "<username>root</username>" +
//                "<password>root</password>" +
//                "</connection>";
//
//        DatabaseMeta srcDatabaseMeta = new DatabaseMeta(mysqL_src);
//
//        //创建转换元信息
//        TransMeta transMeta = new TransMeta();
//        transMeta.setName("获取查询结果");
//
//        //设置源和目标
//        transMeta.addDatabase(srcDatabaseMeta);
//
//		/*
//		 * 创建  表输入->表输出
//		 * 同时将两个步骤连接起来
//		 */
//        PluginRegistry registry = PluginRegistry.getInstance();
//        TableInputMeta tableInputMeta = new TableInputMeta();
//        String tableInputPluginId = registry.getPluginId(StepPluginType.class,
//                tableInputMeta);
//
//        tableInputMeta.setDatabaseMeta(srcDatabaseMeta);
//        //设置查询条件
//        String selectSql = "select id,name from user_info_src";
//        tableInputMeta.setSQL(selectSql);
//
//        StepMeta tableInputStep = new StepMeta(tableInputPluginId,
//                "tableInput", (StepMetaInterface) tableInputMeta);
//        transMeta.addStep(tableInputStep);
//
//        //复制记录到结果
//        RowsToResultMeta rowsToResultMeta = new RowsToResultMeta();
//        String rowsToResultMetaPluginId = registry.getPluginId(StepPluginType.class, rowsToResultMeta);
//
//        //添加步骤到转换中
//        StepMeta rowsToResultStep = new StepMeta(rowsToResultMetaPluginId, "rowsToResult", (StepMetaInterface)rowsToResultMeta);
//        transMeta.addStep(rowsToResultStep);
//
//        //添加hop把两个步骤关联起来
//        transMeta.addTransHop(new TransHopMeta(tableInputStep, rowsToResultStep));
//
//        Trans trans = new Trans(transMeta);
//
//        //执行转换
//        trans.execute(null);
//
//        //等待完成
//        trans.waitUntilFinished();
//        if (trans.getErrors() > 0) {
//            System.out.println("交换出错.");
//            return;
//        }else{
//            Result result = trans.getResult();
//            List<RowMetaAndData> rows = result.getRows(); //获取数据
//            for (RowMetaAndData row : rows) {
//                RowMetaInterface rowMeta = row.getRowMeta(); //获取列的元数据信息
//                String[] fieldNames = rowMeta.getFieldNames();
//                Object[] datas = row.getData();
//                for (int i = 0; i < fieldNames.length; i++) {
//                    System.out.println(fieldNames[i]+"="+datas[i]);
//                }
//            }
//        }
//    }
//
//    /**
//     * 获取查询字段值
//     * @throws KettleException
//     */
//    @Test
//    public void getVariableFromSQL() throws KettleException{
//        log.info("start");
//        //源数据库连接
//        String mysqL_src = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
//                "<connection>" +
//                "<name>mysqL_src</name>" +
//                "<server>192.168.10.64</server>" +
//                "<type>MYSQL</type>" +
//                "<access>Native</access>" +
//                "<database>test</database>" +
//                "<port>3306</port>" +
//                "<username>root</username>" +
//                "<password>root</password>" +
//                "</connection>";
//
//        DatabaseMeta srcDatabaseMeta = new DatabaseMeta(mysqL_src);
//
//        //创建转换元信息
//        TransMeta transMeta = new TransMeta();
//        transMeta.setName("获取查询字段值");
//
//        //设置源和目标
//        transMeta.addDatabase(srcDatabaseMeta);
//
//		/*
//		 * 创建  表输入->表输出
//		 * 同时将两个步骤连接起来
//		 */
//        PluginRegistry registry = PluginRegistry.getInstance();
//        TableInputMeta tableInputMeta = new TableInputMeta();
//        String tableInputPluginId = registry.getPluginId(StepPluginType.class,
//                tableInputMeta);
//
//        tableInputMeta.setDatabaseMeta(srcDatabaseMeta);
//        //设置查询条件
//        String selectSql = "select id,name from user_info_src";
//        tableInputMeta.setSQL(selectSql);
//
//        StepMeta tableInputStep = new StepMeta(tableInputPluginId,
//                "tableInput", (StepMetaInterface) tableInputMeta);
//        transMeta.addStep(tableInputStep);
//
//        //设置变量步
//        SetVariableMeta setVariableMeta = new SetVariableMeta();
//        setVariableMeta.setFieldName(new String[] { "id","name" });
//        setVariableMeta.setVariableName(new String[] { "id_var","name_var" });
//        setVariableMeta.setVariableType(new int[] { SetVariableMeta.VARIABLE_TYPE_ROOT_JOB, SetVariableMeta.VARIABLE_TYPE_ROOT_JOB });
//        setVariableMeta.setDefaultValue(new String[] { "", "" }); //默认值需要设置
//
//        //添加步骤到转换中
//        String setVariablePluginId = registry.getPluginId(StepPluginType.class, setVariableMeta);
//        StepMeta setVariableStep = new StepMeta(setVariablePluginId, "setVariable", (StepMetaInterface) setVariableMeta);
//        transMeta.addStep(setVariableStep);
//
//        //将步骤和上一步关联起来
//        transMeta.addTransHop(new TransHopMeta(tableInputStep, setVariableStep));
//
//        Trans trans = new Trans(transMeta);
//
//        //执行转换
//        trans.execute(null);
//
//        //等待完成
//        trans.waitUntilFinished();
//        if (trans.getErrors() > 0) {
//            System.out.println("交换出错.");
//            return;
//        }else{
//            String nameVar = trans.getVariable("name_var");
//            System.out.println(nameVar);
//        }
//
//    }
//
//}
