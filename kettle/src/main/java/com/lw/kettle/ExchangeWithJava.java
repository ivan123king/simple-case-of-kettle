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
import org.pentaho.di.trans.steps.excelinput.ExcelInputField;
import org.pentaho.di.trans.steps.excelinput.ExcelInputMeta;
import org.pentaho.di.trans.steps.excelinput.SpreadSheetType;
import org.pentaho.di.trans.steps.exceloutput.ExcelField;
import org.pentaho.di.trans.steps.exceloutput.ExcelOutputMeta;
import org.pentaho.di.trans.steps.userdefinedjavaclass.InfoStepDefinition;
import org.pentaho.di.trans.steps.userdefinedjavaclass.UserDefinedJavaClassDef;
import org.pentaho.di.trans.steps.userdefinedjavaclass.UserDefinedJavaClassMeta;

import java.util.ArrayList;
import java.util.List;

/**
 * @author lw
 * @date 2021/11/23 0023
 * @description Java脚本执行
 */
@Slf4j
public class ExchangeWithJava {

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

    @Test
    public void exchangeWithJavaCode() throws KettleException {
        TransMeta transMeta = new TransMeta();
        transMeta.setName("Java 脚本");

        PluginRegistry registry = PluginRegistry.getInstance();

        StepMeta inputStep = getInputStep(transMeta,registry);
        StepMeta javaCodeStep = getJavaStep(transMeta,registry);
        StepMeta outputStep = getOutputStep(transMeta,registry);

        transMeta.addTransHop(new TransHopMeta(inputStep,javaCodeStep));
        transMeta.addTransHop(new TransHopMeta(javaCodeStep,outputStep));

        Trans trans = new Trans(transMeta);

        trans.execute(null);

        trans.waitUntilFinished();
        if (trans.getErrors() > 0) {
            System.out.println("交换出错.");
            return;
        }

    }

    /**
     * 获取java 脚本
     * @param transMeta
     * @param registry
     * @return
     */
    private StepMeta getJavaStep(TransMeta transMeta, PluginRegistry registry){
        UserDefinedJavaClassMeta javaClassMeta = new UserDefinedJavaClassMeta();

        //Java代码
        String sourceCode = "public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {\n" +
                "  if (first) {\n" +
                "    first = false;\n" +
                "\n" +
                "    /* TODO: Your code here. (Using info fields)\n" +
                "\n" +
                "    FieldHelper infoField = get(Fields.Info, \"info_field_name\");\n" +
                "\n" +
                "    RowSet infoStream = findInfoRowSet(\"info_stream_tag\");\n" +
                "\n" +
                "    Object[] infoRow = null;\n" +
                "\n" +
                "    int infoRowCount = 0;\n" +
                "\n" +
                "    // Read all rows from info step before calling getRow() method, which returns first row from any\n" +
                "    // input rowset. As rowMeta for info and input steps varies getRow() can lead to errors.\n" +
                "    while((infoRow = getRowFrom(infoStream)) != null){\n" +
                "\n" +
                "      // do something with info data\n" +
                "      infoRowCount++;\n" +
                "    }\n" +
                "    */\n" +
                "  }\n" +
                "\n" +
                "  Object[] r = getRow();\n" +
                "\n" +
                "  if (r == null) {\n" +
                "    setOutputDone();\n" +
                "    return false;\n" +
                "  }\n" +
                "\n" +
                "  // It is always safest to call createOutputRow() to ensure that your output row's Object[] is large\n" +
                "  // enough to handle any new fields you are creating in this step.\n" +
                "  r = createOutputRow(r, data.outputRowMeta.size());\n" +
                "\n" +
                "  /* TODO: Your code here. (See Sample)\n" +
                "\n" +
                "  // Get the value from an input field\n" +
                "  String foobar = get(Fields.In, \"a_fieldname\").getString(r);\n" +
                "\n" +
                "  foobar += \"bar\";\n" +
                "    \n" +
                "  // Set a value in a new output field\n" +
                "  get(Fields.Out, \"output_fieldname\").setValue(r, foobar);\n" +
                "\n" +
                "  */\n" +
                "\tString name = get(Fields.In,\"name\").getString(r);\n" +
                "\tif(null!=name){\n" +
                "\t\tname = name+\"_new\";\n" +
                "\t}\n" +
                "\tget(Fields.Out,\"new_name\").setValue(r,name);\n" +
                "\n" +
                "  // Send the row on to the next step.\n" +
                "  putRow(data.outputRowMeta, r);\n" +
                "\n" +
                "  return true;\n" +
                "}";

        UserDefinedJavaClassDef classDef = new UserDefinedJavaClassDef(UserDefinedJavaClassDef.ClassType.TRANSFORM_CLASS,"Processor",sourceCode);

        List<UserDefinedJavaClassDef> classDefs = new ArrayList<>();
        classDefs.add(classDef);
        //添加Java脚本到节点中
        javaClassMeta.replaceDefinitions(classDefs);

        List<UserDefinedJavaClassMeta.FieldInfo> fields = new ArrayList<>();

        //定义目标输出字段
        UserDefinedJavaClassMeta.FieldInfo fieldInfo =
                new UserDefinedJavaClassMeta.FieldInfo("new_name",ValueMetaInterface.TYPE_STRING,-1,-1);

        fields.add(fieldInfo);
        javaClassMeta.setFieldInfo(fields);


        String javaClassPluginId = registry.getPluginId(StepPluginType.class, javaClassMeta);
        StepMeta javaClassStep = new StepMeta(javaClassPluginId, "Java 代码", (StepMetaInterface) javaClassMeta);

        javaClassStep.setDraw(true);
        javaClassStep.setLocation(560,304);

        transMeta.addStep(javaClassStep);

        return javaClassStep;
    }


    /**
     * 获取输入
     *
     * @return
     */
    private StepMeta getInputStep(TransMeta transMeta, PluginRegistry registry) {
        ExcelInputMeta inputMeta = new ExcelInputMeta(); // <type>ExcelInput</type>

        //文件路径
        String filePath = "F:\\kette_test\\input\\分组.xlsx";
        String[] fileName = new String[]{filePath};
        inputMeta.setFileName(fileName); // <name>F:\kette_test\input\去除重复记录.xlsx</name>

        String[] fileMasks = new String[1];
        inputMeta.setFileMask(fileMasks); //  <filemask/>

        String[] fileExcludeMasks = new String[1];
        inputMeta.setExcludeFileMask(fileExcludeMasks); // <exclude_filemask/>

        String[] filerequireds = new String[]{"N"};
        inputMeta.setFileRequired(filerequireds); //  <file_required>N</file_required>

        String[] subFolders = new String[]{"N"};
        inputMeta.setIncludeSubFolders(subFolders); // <include_subfolders>N</include_subfolders>

        inputMeta.setSpreadSheetType(SpreadSheetType.POI); // <spreadsheet_type>POI</spreadsheet_type>

        /*
        8.XXX 版本如果excel中有头部，那么需要设置，5.XXX版本不需要
         */
        inputMeta.setStartsWithHeader(true);

        //第二行开始
        int[] startRow = new int[]{1};
        inputMeta.setStartRow(startRow);

        //第一列开始
        int[] startColumn = new int[]{0};
        inputMeta.setStartColumn(startColumn);

        //字段列
        String[] fieldsName = new String[]{"id", "name", "group"};
        int[] fieldsType = new int[]{ValueMetaInterface.TYPE_STRING, ValueMetaInterface.TYPE_STRING, ValueMetaInterface.TYPE_STRING};

        //Excel输入 step下的 <fields> .... </fields>
        ExcelInputField[] excelInputFields = new ExcelInputField[fieldsName.length];

        for (int i = 0; i < excelInputFields.length; i++) {
            excelInputFields[i] = new ExcelInputField();
            excelInputFields[i].setName(fieldsName[i]);
            excelInputFields[i].setType(fieldsType[i]);
            excelInputFields[i].setTrimType(ExcelInputMeta.TYPE_TRIM_NONE);
            excelInputFields[i].setRepeated(false);
        }
        inputMeta.setField(excelInputFields);


        /**
         * 2.添加步骤到交换中
         */
        String inputPluginId = registry.getPluginId(StepPluginType.class, inputMeta);
        StepMeta inputStep = new StepMeta(inputPluginId, "Excel输入", (StepMetaInterface) inputMeta); //<step> --> <name>Excel输入</name>

        inputStep.setDraw(true);
        inputStep.setLocation(425, 309);

        transMeta.addStep(inputStep);

        return inputStep;
    }

    /**
     * 获取输出
     *
     * @return
     */
    private StepMeta getOutputStep(TransMeta transMeta, PluginRegistry registry) {
        ExcelOutputMeta outputMeta = new ExcelOutputMeta(); // <type>ExcelOutput</type>
        outputMeta.setAppend(false); // <append>N</append>
        outputMeta.setHeaderEnabled(true); // <header>Y</header>
        outputMeta.setFooterEnabled(false);// <footer>N</footer>

        //换个名字用于区分spoon运行的输出文件
        outputMeta.setFileName("F:\\kette_test\\output\\java脚本2.xls"); // <name>F:\kette_test\output\java脚本2.xls</name>
        outputMeta.setDoNotOpenNewFileInit(false); // <do_not_open_newfile_init>N</do_not_open_newfile_init>
        outputMeta.setCreateParentFolder(false); // <create_parent_folder>N</create_parent_folder>

        //字段列
        String[] fieldsName = new String[]{"id", "name", "group", "new_name"};
        int[] fieldsType = new int[]{ValueMetaInterface.TYPE_STRING, ValueMetaInterface.TYPE_STRING, ValueMetaInterface.TYPE_STRING, ValueMetaInterface.TYPE_STRING};

        // <fields> ..... </fields>
        ExcelField[] excelFields = new ExcelField[fieldsName.length];

        for (int i = 0; i < excelFields.length; i++) {
            excelFields[i] = new ExcelField();
            excelFields[i].setName(fieldsName[i]);
            excelFields[i].setType(fieldsType[i]);
        }
        outputMeta.setOutputFields(excelFields);

        String outPluginId = registry.getPluginId(StepPluginType.class, outputMeta);
        StepMeta outputStep = new StepMeta(outPluginId, "Excel输出", (StepMetaInterface) outputMeta);// <step> --> <name>Excel输出</name>

        outputStep.setDraw(true);
        outputStep.setLocation(704, 304);

        transMeta.addStep(outputStep);


        return outputStep;
    }

}
