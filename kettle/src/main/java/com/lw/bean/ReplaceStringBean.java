package com.lw.bean;

import lombok.Data;

/**
 * @author lw
 * @date 2021/8/26 0026
 * @description  替换字符串
 */
@Data
public class ReplaceStringBean {
    //原本的内容
    private String originCon;

    //替换后的内容
    private String replaceCon;

    public void setBean(String originCon,String replaceCon){
        this.originCon = originCon;
        this.replaceCon = replaceCon;
    }

}
