package com.lw.kettle;

import lombok.Data;


/**
 * 字段转换类
 * 
 * @author lenovo
 *
 */
@Data
public class FieldTransfer {

	private String field;

	private String src;

	private String target;

	private boolean regEx = false;
}
