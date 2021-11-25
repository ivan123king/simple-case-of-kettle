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
import org.pentaho.di.trans.steps.sort.SortRows;
import org.pentaho.di.trans.steps.sort.SortRowsMeta;
import org.pentaho.di.trans.steps.uniquerows.UniqueRowsMeta;

/**
 * @author lw
 * @date 2021/11/23 0023
 * @description  去除重复记录
 *
 *
 */
@Slf4j
public class ExchangeWithSortAndUnique {

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
     * 去除重复记录前首先要排序，排序完才可以。
     * 排序记录+去除重复记录 = 唯一行（哈希值）
     * 唯一行（哈希值） 是将每一行都计算一个哈希值，然后比较行hash是否相同，相同则去除；
     * 排序记录+去除重复记录  则是比较排序后相邻的记录是否相同，相同则去除；
     * 理论上来说  hash值这个比较快。
     */
    @Test
    public void exchangeWithSortAndUnique() throws KettleException {

        TransMeta transMeta = new TransMeta();
        transMeta.setName("去除重复记录");

        PluginRegistry registry = PluginRegistry.getInstance();

        StepMeta inputStep = getInputStep(transMeta,registry);
        StepMeta sortStep = getSortRowsStep(transMeta,registry);
        StepMeta uniqueStep = getUniqueStep(transMeta,registry);
        StepMeta outStep = getOutputStep(transMeta,registry);



             /*
        4. 关联步骤
         */
        transMeta.addTransHop(new TransHopMeta(inputStep, sortStep));
        transMeta.addTransHop(new TransHopMeta(sortStep, uniqueStep));
        transMeta.addTransHop(new TransHopMeta(uniqueStep, outStep));


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
     * 去除重复记录
     * @param transMeta
     * @param registry
     * @return
     */
    private StepMeta getUniqueStep(TransMeta transMeta, PluginRegistry registry){
        UniqueRowsMeta uniqueRowsMeta = new UniqueRowsMeta();

        uniqueRowsMeta.setCountRows(false);// <count_rows>N</count_rows>
        uniqueRowsMeta.setRejectDuplicateRow(false);//<reject_duplicate_row>N</reject_duplicate_row>

        uniqueRowsMeta.setCompareFields(new String[]{"id","name","age"});
        uniqueRowsMeta.setCaseInsensitive(new boolean[]{false,false,false});

        String uniqueRowsPluginId = registry.getPluginId(StepPluginType.class, uniqueRowsMeta);
        StepMeta uniqueRowsStep = new StepMeta(uniqueRowsPluginId, "去除重复记录", (StepMetaInterface) uniqueRowsMeta);

        uniqueRowsStep.setDraw(true);
        uniqueRowsStep.setLocation(560,384);

        transMeta.addStep(uniqueRowsStep);

        return  uniqueRowsStep;
    }

    /**
     * 排序记录
     * @param transMeta
     * @param registry
     * @return
     */
    private StepMeta getSortRowsStep(TransMeta transMeta, PluginRegistry registry){
        SortRowsMeta sortRowsMeta = new SortRowsMeta();

        sortRowsMeta.setDirectory("%%java.io.tmpdir%%");//<directory>%%java.io.tmpdir%%</directory>
        sortRowsMeta.setPrefix("out");//<prefix>out</prefix>
        sortRowsMeta.setSortSize("1000000");//<sort_size>1000000</sort_size>
        sortRowsMeta.setCompressFiles(false);//<compress>N</compress>
        //这里也可以看出，sortRowsMeta这个组件好像也可以只允许通过唯一的对象
        sortRowsMeta.setOnlyPassingUniqueRows(false);//<unique_rows>N</unique_rows>

        /*
         <field>
            <name>id</name>
            <ascending>Y</ascending>
            <case_sensitive>N</case_sensitive>
            <collator_enabled>N</collator_enabled>
            <collator_strength>0</collator_strength>
            <presorted>N</presorted>
          </field>
         */
        //排序字段
        sortRowsMeta.setFieldName(new String[]{"id","name","age"});
        //是否升序， true 升序
        sortRowsMeta.setAscending(new boolean[]{true,true,true});
        //是否大小写敏感  false 忽略大小写
        sortRowsMeta.setCaseSensitive(new boolean[]{false,false,false});
        sortRowsMeta.setCollatorEnabled(new boolean[]{false,false,false});
        sortRowsMeta.setCollatorStrength(new int[]{0,0,0});
        sortRowsMeta.setPreSortedField(new boolean[]{false,false,false});

        String sortRowsPluginId = registry.getPluginId(StepPluginType.class, sortRowsMeta);
        StepMeta sortRowsStep = new StepMeta(sortRowsPluginId, "排序记录", (StepMetaInterface) sortRowsMeta);

        sortRowsStep.setDraw(true);
        sortRowsStep.setLocation(448,384);

        transMeta.addStep(sortRowsStep);

        return sortRowsStep;
    }

    /**
     * 获取输入
     *
     * @return
     */
    private StepMeta getInputStep(TransMeta transMeta, PluginRegistry registry) {
        ExcelInputMeta inputMeta = new ExcelInputMeta(); // <type>ExcelInput</type>

        //文件路径
        String filePath = "F:\\kette_test\\input\\去除重复记录.xlsx";
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
        String[] fieldsName = new String[]{"id", "name", "age"};
        int[] fieldsType = new int[]{ValueMetaInterface.TYPE_NUMBER, ValueMetaInterface.TYPE_STRING, ValueMetaInterface.TYPE_NUMBER};

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
        inputStep.setLocation(336,256);

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
        outputMeta.setFileName("F:\\kette_test\\output\\去除重复记录2.xls"); // <name>F:\kette_test\output\字符串剪切替换操作</name>
        outputMeta.setDoNotOpenNewFileInit(false); // <do_not_open_newfile_init>N</do_not_open_newfile_init>
        outputMeta.setCreateParentFolder(false); // <create_parent_folder>N</create_parent_folder>

        //字段列
        String[] fieldsName = new String[]{"id", "name", "age"};
        int[] fieldsType = new int[]{ValueMetaInterface.TYPE_NUMBER, ValueMetaInterface.TYPE_STRING, ValueMetaInterface.TYPE_NUMBER};

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
            if(fieldsName[i].equals("age")){
                excelFields[i].setFormat("0"); // <format>0</format>
            }
        }
        outputMeta.setOutputFields(excelFields);

        String outPluginId = registry.getPluginId(StepPluginType.class, outputMeta);
        StepMeta outputStep = new StepMeta(outPluginId, "Excel输出", (StepMetaInterface) outputMeta);// <step> --> <name>Excel输出</name>

        outputStep.setDraw(true);
        outputStep.setLocation(704,272);

        transMeta.addStep(outputStep);


        return outputStep;
    }
}
