package com.lw.bean;

import com.lw.bean.DatabaseConn;
import com.lw.kettle.FieldTransfer;
import lombok.Data;

@Data
public class ExtractBean {

	/**
	 * 源表数据库连接
	 */
	private DatabaseConn srcDB;

	/**
	 * 源表表名
	 */
	private String[] srcTable = new String[0];

	/**
	 * 源表交换字段类型
	 */
	private String[] srcFields;

	/**
	 * 源表主键
	 */
	private String[] srcPk;

	/**
	 * 目标表的数据库配置
	 */
	private DatabaseConn destDB;

	/**
	 * 目标表
	 */
	private String destTable;

	/**
	 * 目标表字段
	 */
	private String[] destFields;

	/**
	 * 目标表主键
	 */
	private String[] destPk;
	
	/**
	 * 数据转换
	 */
	private FieldTransfer[] fieldTransfers;
}
