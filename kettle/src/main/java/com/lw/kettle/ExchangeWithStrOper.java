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
import org.pentaho.di.trans.steps.replacestring.ReplaceStringMeta;
import org.pentaho.di.trans.steps.stringcut.StringCutMeta;
import org.pentaho.di.trans.steps.stringoperations.StringOperationsMeta;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * @author lw
 * @date 2021/11/22 0022
 * @description
 * 字符串剪切、操作、替换
 */
@Slf4j
public class ExchangeWithStrOper {
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
    public void exchangeOperStr() throws KettleException{

        TransMeta transMeta = new TransMeta();
        transMeta.setName("字符串剪切操作替换"); // <info> -->  <name>字符串剪切操作替换</name>

        PluginRegistry registry = PluginRegistry.getInstance();

        StepMeta inputStep = getInputStep(transMeta,registry);
        StepMeta strCutStep = getStringCutStep(transMeta,registry);
        StepMeta strReplaceStep = getReplaceStringStep(transMeta,registry);
        StepMeta strOperStep = getStringOperationsStep(transMeta,registry);
        StepMeta outputStep = getOutputStep(transMeta,registry);


             /*
        4. 关联步骤
         */
        transMeta.addTransHop(new TransHopMeta(inputStep, strCutStep));
        transMeta.addTransHop(new TransHopMeta(strCutStep, strReplaceStep));
        transMeta.addTransHop(new TransHopMeta(strReplaceStep, strOperStep));
        transMeta.addTransHop(new TransHopMeta(strOperStep, outputStep));


//        try {
//            createKtrFile(transMeta);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }


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
     * 字符串替换
     * @param transMeta
     * @param registry
     * @return
     */
    private StepMeta getReplaceStringStep(TransMeta transMeta, PluginRegistry registry){
        /*
        <fields>
          <field>
            <in_stream_name>description</in_stream_name>
            <out_stream_name>desc</out_stream_name>
            <use_regex>no</use_regex>
            <replace_string>港剧</replace_string>
            <replace_by_string>港台大片</replace_by_string>
            <set_empty_string>N</set_empty_string>
            <replace_field_by_string/>
            <whole_word>no</whole_word>
            <case_sensitive>no</case_sensitive>
            <is_unicode>no</is_unicode>
          </field>
        </fields>
         */
        ReplaceStringMeta replaceStringMeta = new ReplaceStringMeta();
        replaceStringMeta.setFieldInStream(new String[]{"description"});
        replaceStringMeta.setFieldOutStream(new String[]{"desc"});
        replaceStringMeta.setUseRegEx(new int[]{ReplaceStringMeta.USE_REGEX_NO});//是否正则匹配，否
        replaceStringMeta.setReplaceString(new String[]{"港剧"});
        replaceStringMeta.setReplaceByString(new String[]{"港台大片"});
        replaceStringMeta.setEmptyString(new boolean[]{false});
        replaceStringMeta.setFieldReplaceByString(new String[1]);
        replaceStringMeta.setWholeWord(new int[]{ReplaceStringMeta.WHOLE_WORD_NO});
        replaceStringMeta.setCaseSensitive(new int[]{ReplaceStringMeta.CASE_SENSITIVE_NO});
        replaceStringMeta.setIsUnicode(new int[]{ReplaceStringMeta.IS_UNICODE_NO});

        String replaceStringPluginId = registry.getPluginId(StepPluginType.class, replaceStringMeta);
        StepMeta replaceStringStep = new StepMeta(replaceStringPluginId, "字符串替换", (StepMetaInterface) replaceStringMeta);

        replaceStringStep.setDraw(true);
        replaceStringStep.setLocation(592,272);

        transMeta.addStep(replaceStringStep);

        return replaceStringStep;

    }

    /**
     * 字符串操作
     * @param transMeta
     * @param registry
     * @return
     */
    private StepMeta getStringOperationsStep(TransMeta transMeta, PluginRegistry registry){
        StringOperationsMeta stringOperationsMeta = new StringOperationsMeta();

        /*
         <fields>
              <field>
                <in_stream_name>author</in_stream_name>
                <out_stream_name>author_upper</out_stream_name>
                <trim_type>both</trim_type>
                <lower_upper>upper</lower_upper>
                <padding_type>none</padding_type>
                <pad_char/>
                <pad_len/>
                <init_cap>no</init_cap>
                <mask_xml>none</mask_xml>
                <digits>none</digits>
                <remove_special_characters>none</remove_special_characters>
              </field>
            </fields>
         */
        stringOperationsMeta.setFieldInStream(new String[]{"author"});
        stringOperationsMeta.setFieldOutStream(new String[]{"author_upper"});
        stringOperationsMeta.setTrimType(new int[]{ValueMetaInterface.TRIM_TYPE_BOTH});
        /*
        在Kettle源码高版本(9.XXX) 中，lowerUpper这个属性变成了string类型，直接使用none, upper, lower 三个属性值即可
        写这个代码之前引用的是5.XXX 版本的包,但是spoon工具用的是8.XXX 版本，所以导致有点不匹配,此处换成8.2.0.0-342的包
        8.2.0.0-342 这个包是我下载的源码，然后自己打包编译的，忘了是哪个版本的源码了，应该是8.2.0.0-XXX的。\
        之前使用5.XXX版本的jar包，是因为5.XXX版本为了公司要求改了很多源码，对优化和支持等做了修改。

        kettle后续有些meta是没有了，比如8.XXX版本中 ConcatFieldMeta这个类，就需要靠plugins中找到，要引入插件的，这应该也是后续kettle的方向，
        插件式集成，方便后续扩展等。
         */

        stringOperationsMeta.setLowerUpper(new int[]{StringOperationsMeta.LOWER_UPPER_UPPER});
        stringOperationsMeta.setPaddingType(new int[]{StringOperationsMeta.PADDING_NONE});
        stringOperationsMeta.setPadChar(new String[1]);
        stringOperationsMeta.setPadLen(new String[1]);
        stringOperationsMeta.setInitCap(new int[]{StringOperationsMeta.INIT_CAP_NO});
        stringOperationsMeta.setMaskXML(new int[]{StringOperationsMeta.MASK_NONE});
        stringOperationsMeta.setDigits(new int[]{StringOperationsMeta.DIGITS_NONE});
        stringOperationsMeta.setRemoveSpecialCharacters(new int[]{StringOperationsMeta.REMOVE_SPECIAL_CHARACTERS_NONE});

        String stringOperationsPluginId = registry.getPluginId(StepPluginType.class, stringOperationsMeta);
        StepMeta stringOperationsStep = new StepMeta(stringOperationsPluginId, "字符串操作", (StepMetaInterface) stringOperationsMeta);

        stringOperationsStep.setDraw(true);
        stringOperationsStep.setLocation(720,272);

        transMeta.addStep(stringOperationsStep);

        return stringOperationsStep;
    }


    /**
     * 剪切字符串
     * @return
     */
    private StepMeta getStringCutStep(TransMeta transMeta, PluginRegistry registry){

        /**
         * 1. 组装meta
         */
        StringCutMeta stringCutMeta = new StringCutMeta();
        /*
         <fields>
              <field>
                <in_stream_name>title</in_stream_name>
                <out_stream_name>title_begin</out_stream_name>
                <cut_from>0</cut_from>
                <cut_to>1</cut_to>
              </field>
         </fields>
         */
        stringCutMeta.setFieldInStream(new String[]{"title"});
        stringCutMeta.setFieldOutStream(new String[]{"title_begin"});
        stringCutMeta.setCutFrom(new String[]{"0"});
        stringCutMeta.setCutTo(new String[]{"1"});

        /**
         * 2. 添加step到交换中
         */
        String stringCutPluginId = registry.getPluginId(StepPluginType.class, stringCutMeta);
        StepMeta stringCutStep = new StepMeta(stringCutPluginId, "剪切字符串", (StepMetaInterface) stringCutMeta);

        //设置在图形化界面中展示位置
        stringCutStep.setDraw(true);
        stringCutStep.setLocation(496,272);

        transMeta.addStep(stringCutStep);

        return stringCutStep;
    }

    /**
     * 获取输入
     *
     * @return
     */
    private StepMeta getInputStep(TransMeta transMeta, PluginRegistry registry) {
        ExcelInputMeta inputMeta = new ExcelInputMeta(); // <type>ExcelInput</type>

        //文件路径
        String filePath = "F:\\kette_test\\input\\字符串剪切操作替换.xlsx";
        String[] fileName = new String[]{filePath};
        inputMeta.setFileName(fileName); // <name>F:\kette_test\input\字符串剪切操作替换.xlsx</name>

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
        String[] fieldsName = new String[]{"id", "title", "author", "description"};
        int[] fieldsType = new int[]{ValueMetaInterface.TYPE_NUMBER, ValueMetaInterface.TYPE_STRING, ValueMetaInterface.TYPE_STRING, ValueMetaInterface.TYPE_STRING};

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
        inputStep.setLocation(384,176);

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
        outputMeta.setFileName("F:\\kette_test\\output\\字符串剪切替换操作2"); // <name>F:\kette_test\output\字符串剪切替换操作</name>
        outputMeta.setExtension("xls");// <extention>xls</extention>
        outputMeta.setDoNotOpenNewFileInit(false); // <do_not_open_newfile_init>N</do_not_open_newfile_init>
        outputMeta.setCreateParentFolder(false); // <create_parent_folder>N</create_parent_folder>

        //字段列
        String[] fieldsName = new String[]{"id", "title", "author", "description", "title_begin", "desc", "author_upper"};
        int[] fieldsType = new int[]{ValueMetaInterface.TYPE_NUMBER, ValueMetaInterface.TYPE_STRING,
                ValueMetaInterface.TYPE_STRING, ValueMetaInterface.TYPE_STRING, ValueMetaInterface.TYPE_STRING,
                ValueMetaInterface.TYPE_STRING, ValueMetaInterface.TYPE_STRING};

        // <fields> ..... </fields>
        ExcelField[] excelFields = new ExcelField[fieldsName.length];

        for (int i = 0; i < excelFields.length; i++) {
            excelFields[i] = new ExcelField();
            excelFields[i].setName(fieldsName[i]);
            excelFields[i].setType(fieldsType[i]);

            /*
             <field>
        <name>id</name>
        <type>Number</type>
        <format>0</format>
      </field>
             */
            if(fieldsName[i].equals("id")){
                excelFields[i].setFormat("0"); // <format>0</format>
            }
        }
        outputMeta.setOutputFields(excelFields);

        String outPluginId = registry.getPluginId(StepPluginType.class, outputMeta);
        StepMeta outputStep = new StepMeta(outPluginId, "Excel输出", (StepMetaInterface) outputMeta);// <step> --> <name>Excel输出</name>

        outputStep.setDraw(true);
        outputStep.setLocation(848,176);

        transMeta.addStep(outputStep);


        return outputStep;
    }

    private void createKtrFile(TransMeta transMeta) throws KettleException,IOException{
        String xml = transMeta.getXML();

        String filePath = "F:\\kette_test\\错误\\str.ktr";
        BufferedWriter bw = new BufferedWriter(new FileWriter(new File(filePath)));

        bw.write(xml);

        bw.close();


    }
}
