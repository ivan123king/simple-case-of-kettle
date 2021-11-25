//package com.lw.kettle;
//
//import lombok.Data;
//import lombok.extern.slf4j.Slf4j;
//import org.junit.Before;
//import org.junit.Test;
//import org.pentaho.di.core.KettleEnvironment;
//import org.pentaho.di.core.exception.KettleException;
//import org.pentaho.di.core.plugins.PluginRegistry;
//import org.pentaho.di.core.plugins.StepPluginType;
//import org.pentaho.di.core.row.ValueMetaInterface;
//import org.pentaho.di.core.util.EnvUtil;
//import org.pentaho.di.trans.Trans;
//import org.pentaho.di.trans.TransHopMeta;
//import org.pentaho.di.trans.TransMeta;
//import org.pentaho.di.trans.step.StepMeta;
//import org.pentaho.di.trans.step.StepMetaInterface;
//import org.pentaho.di.trans.steps.calculator.CalculatorMeta;
//import org.pentaho.di.trans.steps.calculator.CalculatorMetaFunction;
//import org.pentaho.di.trans.steps.concatfields.ConcatFieldsMeta;
//import org.pentaho.di.trans.steps.excelinput.ExcelInputField;
//import org.pentaho.di.trans.steps.excelinput.ExcelInputMeta;
//import org.pentaho.di.trans.steps.excelinput.SpreadSheetType;
//import org.pentaho.di.trans.steps.exceloutput.ExcelField;
//import org.pentaho.di.trans.steps.exceloutput.ExcelOutputMeta;
//import org.pentaho.di.trans.steps.textfileoutput.TextFileField;
//
///**
// * @author lw
// * @date 2021/11/19 0019
// * @description
// */
//@Slf4j
//public class ExchangeWithComputer {
//
//    @Before
//    public void before() {
//        try {
//            //初始化环境
//            EnvUtil.environmentInit();
//            KettleEnvironment.init();
//        } catch (KettleException e) {
//            log.error("", e);
//        }
//    }
//
//    /**
//     * 转换->计算器
//     */
//    @Test
//    public void exchangeWithComputer() throws  KettleException {
//
//        /*
//        1. Excel输入
//         */
//        ExcelInputMeta inputMeta = getInputMeta();
//
//        /*
//        2. concat fields节点
//         */
//        ConcatFieldsMeta concatFieldsMeta = getConcatFieldsMeta();
//
//        /*
//        3. 计算器节点
//         */
//        CalculatorMeta calculatorMeta = getCalculatorMeta();
//
//        /*
//        4. excel输出
//         */
//        ExcelOutputMeta outputMeta = getOutputMeta();
//
//
//         /*
//        5. 添加步骤
//         */
//        TransMeta transMeta = new TransMeta();
//        transMeta.setName("计算器"); // <info> -->  <name>计算器</name>
//
//        PluginRegistry registry = PluginRegistry.getInstance();
//
//        String inputPluginId = registry.getPluginId(StepPluginType.class, inputMeta);
//        StepMeta inputStep = new StepMeta(inputPluginId, "Excel输入", (StepMetaInterface) inputMeta); //<step> --> <name>Excel输入</name>
//        transMeta.addStep(inputStep);
//
//        String concatFieldPluginId = registry.getPluginId(StepPluginType.class, concatFieldsMeta);
//        StepMeta concatFieldStep = new StepMeta(concatFieldPluginId, "Concat fields", (StepMetaInterface) concatFieldsMeta); //<step> --> <name>Excel输入</name>
//        transMeta.addStep(concatFieldStep);
//
//        String calculatorPluginId = registry.getPluginId(StepPluginType.class, calculatorMeta);
//        StepMeta calculatorStep = new StepMeta(calculatorPluginId, "计算器", (StepMetaInterface) calculatorMeta); //<step> --> <name>Excel输入</name>
//        transMeta.addStep(calculatorStep);
//
//
//        String outPluginId = registry.getPluginId(StepPluginType.class, outputMeta);
//        StepMeta outputStep = new StepMeta(outPluginId, "Excel输出", (StepMetaInterface) outputMeta);// <step> --> <name>Excel输出</name>
//        transMeta.addStep(outputStep);
//
//
//        /*
//        4. 关联步骤
//         */
//        transMeta.addTransHop(new TransHopMeta(inputStep, concatFieldStep));
//        transMeta.addTransHop(new TransHopMeta(concatFieldStep, calculatorStep));
//        transMeta.addTransHop(new TransHopMeta(calculatorStep, outputStep));
//
//        /*
//        5.执行
//         */
//        Trans trans = new Trans(transMeta);
//
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
//    }
//
//
//    /**
//     * 获取字符串连接
//     * @return
//     */
//    private ConcatFieldsMeta getConcatFieldsMeta(){
//        ConcatFieldsMeta concatFieldsMeta = new ConcatFieldsMeta(); //<type>ConcatFields</type>
////        concatFieldsMeta.setDefault();
//
//        //设置连接分割符
//        concatFieldsMeta.setSeparator("-"); // <separator>-</separator>
//
//        //设置封闭符
//        concatFieldsMeta.setEnclosure("\""); // <enclosure>"</enclosure>
//        concatFieldsMeta.setEnclosureForced(false); // <enclosure_forced>N</enclosure_forced>
//        concatFieldsMeta.setEnclosureFixDisabled(false); // <enclosure_fix_disabled>N</enclosure_fix_disabled>
//
//        concatFieldsMeta.setHeaderEnabled(false); // <header>N</header>
//
//        concatFieldsMeta.setFooterEnabled(false); // <footer>N</footer>
//
//        //此处有默认值，可不设置
//        concatFieldsMeta.setFileFormat("DOS"); // <format>DOS</format>
//
//        concatFieldsMeta.setFileCompression("None"); // <compression>None</compression>
//
//        concatFieldsMeta.setFileNameInField(false); // <fileNameInField>N</fileNameInField>
//
//        concatFieldsMeta.setCreateParentFolder(true);// <create_parent_folder>Y</create_parent_folder>
//
//
//        //设置源字段列
//        TextFileField[] textFileFields = new TextFileField[2];
//
//        /*
//        <field>
//        <name>firstName</name>
//        <type>None</type>
//        <format/>
//        <currency/>
//        <decimal/>
//        <group/>
//        <nullif/>
//        <trim_type>none</trim_type>
//        <length>-1</length>
//        <precision>-1</precision>
//      </field>
//         */
//        textFileFields[0] = new TextFileField();
//        textFileFields[0].setName("firstName");
//        textFileFields[0].setType(ValueMetaInterface.TYPE_NONE);
//        textFileFields[0].setTrimType(ValueMetaInterface.TRIM_TYPE_NONE);
//        textFileFields[0].setLength(-1);
//
//        /*
//         <field>
//        <name>lastName</name>
//        <type>None</type>
//        <format/>
//        <currency/>
//        <decimal/>
//        <group/>
//        <nullif/>
//        <trim_type>none</trim_type>
//        <length>-1</length>
//        <precision>-1</precision>
//      </field>
//         */
//        textFileFields[1] = new TextFileField();
//        textFileFields[1].setName("lastName"); // <name>lastName</name>
//        textFileFields[1].setType(ValueMetaInterface.TYPE_NONE); // <type>None</type>
//        textFileFields[1].setTrimType(ValueMetaInterface.TRIM_TYPE_NONE);
//        textFileFields[1].setLength(-1); // <fields> --> <field> --> <length>-1</length>
//
//        concatFieldsMeta.setOutputFields(textFileFields);
//
//        //设置目标字段
//        /*
//        <ConcatFields>
//      <targetFieldName>name</targetFieldName>
//      <targetFieldLength>0</targetFieldLength>
//      <removeSelectedFields>N</removeSelectedFields>
//    </ConcatFields>
//         */
//        concatFieldsMeta.setTargetFieldName("name"); // <targetFieldName>name</targetFieldName>
//        concatFieldsMeta.setTargetFieldLength(0); // <targetFieldLength>0</targetFieldLength>
//        concatFieldsMeta.setRemoveSelectedFields(false);// <removeSelectedFields>N</removeSelectedFields>
//
//        return  concatFieldsMeta;
//    }
//
//    /**
//     * 获取计算器
//     * @return
//     */
//    private CalculatorMeta getCalculatorMeta(){
//        CalculatorMeta calculatorMeta = new CalculatorMeta(); // <type>Calculator</type>
//
//
//        CalculatorMetaFunction[] metaFunctions = new CalculatorMetaFunction[3];
//
//        metaFunctions[0] = new CalculatorMetaFunction();
//        metaFunctions[0].setFieldName("quarter"); // <field_name>quarter</field_name>
//        metaFunctions[0].setCalcType(CalculatorMetaFunction.CALC_QUARTER_OF_DATE);// <calc_type>QUARTER_OF_DATE</calc_type>
//        metaFunctions[0].setFieldA("birthday"); // <field_a>birthday</field_a>
//        metaFunctions[0].setValueType(ValueMetaInterface.TYPE_NONE); // <value_type>None</value_type>
//        metaFunctions[0].setRemovedFromResult(false); // <remove>N</remove>
//
//        metaFunctions[1] = new CalculatorMetaFunction();
//        metaFunctions[1].setFieldName("week_of_day");
//        metaFunctions[1].setCalcType(CalculatorMetaFunction.CALC_DAY_OF_WEEK);
//        metaFunctions[1].setFieldA("birthday");
//        metaFunctions[1].setValueType(ValueMetaInterface.TYPE_NONE);
//        metaFunctions[1].setRemovedFromResult(false);
//
//        metaFunctions[2] = new CalculatorMetaFunction();
//        metaFunctions[2].setFieldName("account");
//        metaFunctions[2].setCalcType(CalculatorMetaFunction.CALC_MULTIPLY);
//        metaFunctions[2].setFieldA("working_life");
//        metaFunctions[2].setFieldB("yearly_salary");
//        metaFunctions[2].setValueType(ValueMetaInterface.TYPE_NONE);
//        metaFunctions[2].setRemovedFromResult(false);
//
//        calculatorMeta.setCalculation(metaFunctions);
//
//        return  calculatorMeta;
//    }
//
//
//    /**
//     * 获取输入
//     *
//     * @return
//     */
//    private ExcelInputMeta getInputMeta() {
//        ExcelInputMeta inputMeta = new ExcelInputMeta(); // <type>ExcelInput</type>
//
//        //文件路径
//        String filePath = "F:\\kette_test\\input\\计算器.xlsx";
//        String[] fileName = new String[]{filePath};
//        inputMeta.setFileName(fileName); // <name>F:\kette_test\input\计算器.xlsx</name>
//
//        String[] fileMasks = new String[1];
//        inputMeta.setFileMask(fileMasks); //  <filemask/>
//
//        String[] fileExcludeMasks = new String[1];
//        inputMeta.setExcludeFileMask(fileExcludeMasks); // <exclude_filemask/>
//
//        String[] filerequireds = new String[]{"N"};
//        inputMeta.setFileRequired(filerequireds); //  <file_required>N</file_required>
//
//        String[] subFolders = new String[]{"N"};
//        inputMeta.setIncludeSubFolders(subFolders); // <include_subfolders>N</include_subfolders>
//
//        inputMeta.setSpreadSheetType(SpreadSheetType.POI); // <spreadsheet_type>POI</spreadsheet_type>
//
//        //第二行开始
//        int[] startRow = new int[]{1};
//        inputMeta.setStartRow(startRow);
//
//        //第一列开始
//        int[] startColumn = new int[]{0};
//        inputMeta.setStartColumn(startColumn);
//
//
//        //字段列
//        String[] fieldsName = new String[]{"id", "firstName", "lastName", "birthday", "working_life", "yearly_salary"};
//        int[] fieldsType = new int[]{ValueMetaInterface.TYPE_NUMBER, ValueMetaInterface.TYPE_STRING, ValueMetaInterface.TYPE_STRING, ValueMetaInterface.TYPE_DATE, ValueMetaInterface.TYPE_NUMBER, ValueMetaInterface.TYPE_NUMBER};
//
//        //Excel输入 step下的 <fields> .... </fields>
//        ExcelInputField[] excelInputFields = new ExcelInputField[fieldsName.length];
//
//        for (int i = 0; i < excelInputFields.length; i++) {
//            excelInputFields[i] = new ExcelInputField();
//            excelInputFields[i].setName(fieldsName[i]);
//            excelInputFields[i].setType(fieldsType[i]);
//            excelInputFields[i].setTrimType(ExcelInputMeta.TYPE_TRIM_NONE);
//            excelInputFields[i].setRepeated(false);
//        }
//        inputMeta.setField(excelInputFields);
//
//        return inputMeta;
//    }
//
//    /**
//     * 获取输出
//     *
//     * @return
//     */
//    private ExcelOutputMeta getOutputMeta() {
//        ExcelOutputMeta outputMeta = new ExcelOutputMeta(); // <type>ExcelOutput</type>
//        outputMeta.setAppend(false); // <append>N</append>
//        outputMeta.setHeaderEnabled(true); // <header>Y</header>
//        outputMeta.setFooterEnabled(false);// <footer>N</footer>
//
//        outputMeta.setFileName("F:\\kette_test\\output\\excel输出-计算器"); // <name>F:\kette_test\output\计算器</name>
//        outputMeta.setExtension("xls");// <extention>xls</extention>
//        outputMeta.setDoNotOpenNewFileInit(false); // <do_not_open_newfile_init>N</do_not_open_newfile_init>
//        outputMeta.setCreateParentFolder(false); // <create_parent_folder>N</create_parent_folder>
//
//        String[] fieldsName = new String[]{"id", "firstName", "lastName", "birthday", "working_life", "yearly_salary","name","quarter","week_of_day","account"};
//        int[] fieldsType = new int[]{ValueMetaInterface.TYPE_NUMBER, ValueMetaInterface.TYPE_STRING, ValueMetaInterface.TYPE_STRING, ValueMetaInterface.TYPE_DATE, ValueMetaInterface.TYPE_NUMBER, ValueMetaInterface.TYPE_NUMBER,ValueMetaInterface.TYPE_NUMBER,ValueMetaInterface.TYPE_INTEGER,ValueMetaInterface.TYPE_INTEGER,ValueMetaInterface.TYPE_NUMBER};
//
//        // <fields> ..... </fields>
//        ExcelField[] excelFields = new ExcelField[fieldsName.length];
//
//        for (int i = 0; i < excelFields.length; i++) {
//            excelFields[i] = new ExcelField();
//            excelFields[i].setName(fieldsName[i]);
//            excelFields[i].setType(fieldsType[i]);
//
//            /*
//             <field>
//        <name>id</name>
//        <type>Number</type>
//        <format>0</format>
//      </field>
//             */
//            if(fieldsName[i].equals("id")){
//                excelFields[i].setFormat("0"); // <format>0</format>
//            }
//        }
//        outputMeta.setOutputFields(excelFields);
//
//        return outputMeta;
//    }
//}
//
