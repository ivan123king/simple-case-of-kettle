package com.lw.bean;

import lombok.Data;

@Data
public class DatabaseConn {

	/** 数据库服务器IP地址 */
	private String server;

	/** 数据库类型 */
	private String type;

	/** 访问类型（Native,ODBC,JNDI） */
	private String access = "Native";

	/** 数据库名称 */
	private String database;

	/** 连接端口 */
	private String port;

	/** 连接用户名 */
	private String username;

	/** 连接密码 */
	private String password;

}
