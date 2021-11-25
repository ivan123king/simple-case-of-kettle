package com.lw.kettle;

import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.csvinput.CsvInputMeta;
import org.pentaho.di.trans.steps.excelinput.ExcelInputField;
import org.pentaho.di.trans.steps.excelinput.ExcelInputMeta;
import org.pentaho.di.trans.steps.excelinput.SpreadSheetType;
import org.pentaho.di.trans.steps.exceloutput.ExcelField;
import org.pentaho.di.trans.steps.exceloutput.ExcelOutputMeta;
import org.pentaho.di.trans.steps.textfileinput.TextFileInputField;

/**
 * @author lw
 * @date 2021/11/18 0018
 * @description
 */
@Slf4j
public class ExchangeWithExcel {

    @Before
    public void before() {
        try {
            //初始化环境
            EnvUtil.environmentInit();
            KettleEnvironment.init();
        } catch (KettleException e) {
            log.error("", e);
        }
    }

    /**
     * excel之间交换
     */
    @Test
    public void exchangeExcel2Excel() throws KettleException {

        /*
        1.excel输入
         */
        ExcelInputMeta inputMeta = new ExcelInputMeta();

        //文件路径
        String filePath = "F:\\kette_test\\input\\person.xlsx";
        String[] fileName = new String[]{filePath};
        inputMeta.setFileName(fileName);

        String[] fileMasks = new String[1];
        inputMeta.setFileMask(fileMasks);

        String[] fileExcludeMasks = new String[1];
        inputMeta.setExcludeFileMask(fileExcludeMasks);

        String[] filerequireds = new String[]{"N"};
        inputMeta.setFileRequired(filerequireds);

        String[] subFolders = new String[]{"N"};
        inputMeta.setIncludeSubFolders(subFolders);

        inputMeta.setSpreadSheetType(SpreadSheetType.POI);

        //第二行开始
        int[] startRow = new int[]{1};
        inputMeta.setStartRow(startRow);

        //第一列开始
        int[] startColumn = new int[]{0};
        inputMeta.setStartColumn(startColumn);


        //字段列
        ExcelInputField[] excelInputFields = new ExcelInputField[3];
        excelInputFields[0] = new ExcelInputField();
        excelInputFields[0].setName("id");
        excelInputFields[0].setType(ValueMetaInterface.TYPE_STRING);
        excelInputFields[0].setTrimType(ExcelInputMeta.TYPE_TRIM_NONE);
        excelInputFields[0].setRepeated(false);

        excelInputFields[1] = new ExcelInputField();
        excelInputFields[1].setName("name");
        excelInputFields[1].setType(ValueMetaInterface.TYPE_STRING);
        excelInputFields[1].setTrimType(ExcelInputMeta.TYPE_TRIM_NONE);
        excelInputFields[1].setRepeated(false);

        excelInputFields[2] = new ExcelInputField();
        excelInputFields[2].setName("age");
        excelInputFields[2].setType(ValueMetaInterface.TYPE_INTEGER);
        excelInputFields[2].setTrimType(ExcelInputMeta.TYPE_TRIM_NONE);
        excelInputFields[2].setRepeated(false);

        inputMeta.setField(excelInputFields);


        /*
        2. excel输出
         */
        ExcelOutputMeta outputMeta = new ExcelOutputMeta();
        outputMeta.setAppend(false);
        outputMeta.setHeaderEnabled(true);
        outputMeta.setFooterEnabled(false);

        outputMeta.setFileName("F:\\kette_test\\output\\excel输出.xls");
        outputMeta.setDoNotOpenNewFileInit(false);
        outputMeta.setCreateParentFolder(false);

        ExcelField[] excelFields = new ExcelField[3];
        excelFields[0] = new ExcelField();
        excelFields[0].setName("id");
        excelFields[0].setType(ValueMetaInterface.TYPE_STRING);

        excelFields[1] = new ExcelField();
        excelFields[1].setName("name");
        excelFields[1].setType(ValueMetaInterface.TYPE_STRING);

        excelFields[2] = new ExcelField();
        excelFields[2].setName("age");
        excelFields[2].setType(ValueMetaInterface.TYPE_INTEGER);
        excelFields[2].setFormat("0");

        outputMeta.setOutputFields(excelFields);

        /*
        3. 添加步骤
         */
        TransMeta transMeta = new TransMeta();
        transMeta.setName("excel交换");

        PluginRegistry registry = PluginRegistry.getInstance();

        String inputPluginId = registry.getPluginId(StepPluginType.class, inputMeta);
        StepMeta inputStep = new StepMeta(inputPluginId, "excel-input", (StepMetaInterface) inputMeta);

        //给步骤添加在spoon工具中的显示位置
        inputStep.setDraw(true);
        inputStep.setLocation(200, 200);
        //将步骤添加进去
        transMeta.addStep(inputStep);


        String outPluginId = registry.getPluginId(StepPluginType.class, outputMeta);
        StepMeta outputStep = new StepMeta(outPluginId, "excel-output", (StepMetaInterface) outputMeta);

        //给步骤添加在spoon工具中的显示位置
        outputStep.setDraw(true);
        outputStep.setLocation(300, 200);
        transMeta.addStep(outputStep);


        /*
        4. 关联步骤
         */
        transMeta.addTransHop(new TransHopMeta(inputStep, outputStep));

        /*
        5.执行
         */
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
     * csv 到excel交换
     * @throws KettleException
     */
    @Test
    public void exchangeCsv2Excel() throws KettleException{
        /*
        1.输入
         */
        CsvInputMeta inputMeta = new CsvInputMeta();

        /**
         *  文件内容：
         *   id,name,age,set
             id1,name1,20,1
             id2,name2,21,1
             id3,name3,22,1
             id4,name4,23,0
             id5,name5,24,0
             id6,name6,25,0
         */
        String filePath = "F:\\kette_test\\input\\person.csv";
        inputMeta.setFilename(filePath);

        //设置列分割符
        inputMeta.setDelimiter(",");

        //设置封闭符
        inputMeta.setEnclosure("\"");

        //表头
        inputMeta.setHeaderPresent(true);

        inputMeta.setBufferSize("50000");

        //字段列
        String[] fieldsName = new String[]{"id","name","age","set"};
        TextFileInputField[] inputFields = new TextFileInputField[fieldsName.length];
        inputFields[0] = new TextFileInputField(fieldsName[0],-1,3);
        inputFields[0].setType(ValueMetaInterface.TYPE_STRING);
        inputFields[0].setDecimalSymbol(".");
        inputFields[0].setGroupSymbol(",");

        inputFields[1] = new TextFileInputField(fieldsName[1],-1,5);
        inputFields[1].setType(ValueMetaInterface.TYPE_STRING);
        inputFields[1].setDecimalSymbol(".");
        inputFields[1].setGroupSymbol(",");

        inputFields[2] = new TextFileInputField(fieldsName[2],-1,15);
        inputFields[2].setType(ValueMetaInterface.TYPE_INTEGER);
        inputFields[2].setFormat("#");
        inputFields[2].setDecimalSymbol(".");
        inputFields[2].setGroupSymbol(",");
        inputFields[2].setPrecision(0);

        inputFields[3] = new TextFileInputField(fieldsName[3],-1,15);
        inputFields[3].setType(ValueMetaInterface.TYPE_INTEGER);
        inputFields[3].setFormat("#");
        inputFields[3].setDecimalSymbol(".");
        inputFields[3].setGroupSymbol(",");
        inputFields[3].setPrecision(0);

        inputMeta.setInputFields(inputFields);

        /*
        2.输出
         */
        ExcelOutputMeta outputMeta = new ExcelOutputMeta();
        outputMeta.setAppend(false);
        outputMeta.setHeaderEnabled(true);
        outputMeta.setFooterEnabled(false);

        outputMeta.setFileName("F:\\kette_test\\output\\person_2");
        //设置扩展名
        outputMeta.setExtension("xls");
        outputMeta.setDoNotOpenNewFileInit(false);
        outputMeta.setCreateParentFolder(false);

        outputMeta.allocate(fieldsName.length);

        ExcelField[] excelFields = outputMeta.getOutputFields();
        excelFields[0] = new ExcelField();
        excelFields[0].setName(fieldsName[0]);
        excelFields[0].setType(ValueMetaInterface.TYPE_STRING);

        excelFields[1] = new ExcelField();
        excelFields[1].setName(fieldsName[1]);
        excelFields[1].setType(ValueMetaInterface.TYPE_STRING);

        excelFields[2] = new ExcelField();
        excelFields[2].setName(fieldsName[2]);
        excelFields[2].setType(ValueMetaInterface.TYPE_INTEGER);
        excelFields[2].setFormat("0");

        excelFields[3] = new ExcelField();
        excelFields[3].setName(fieldsName[3]);
        excelFields[3].setType(ValueMetaInterface.TYPE_INTEGER);
        excelFields[3].setFormat("0");

        outputMeta.setOutputFields(excelFields);

        /*
        3. 步骤添加
         */
        TransMeta transMeta = new TransMeta();
        transMeta.setName("csv2excel交换");

        PluginRegistry registry = PluginRegistry.getInstance();
        String inputPluginId = registry.getPluginId(StepPluginType.class, inputMeta);
        StepMeta inputStep = new StepMeta(inputPluginId, "csv-input", (StepMetaInterface) inputMeta);

        //给步骤添加在spoon工具中的显示位置
        inputStep.setDraw(true);
        inputStep.setLocation(200, 200);
        //将步骤添加进去
        transMeta.addStep(inputStep);


        String outPluginId = registry.getPluginId(StepPluginType.class, outputMeta);
        StepMeta outputStep = new StepMeta(outPluginId, "excel-output", (StepMetaInterface) outputMeta);

        //给步骤添加在spoon工具中的显示位置
        outputStep.setDraw(true);
        outputStep.setLocation(300, 200);
        transMeta.addStep(outputStep);
        /*
        4.步骤关联
         */
        transMeta.addTransHop(new TransHopMeta(inputStep, outputStep));

        /*
        5.执行交换
         */
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
