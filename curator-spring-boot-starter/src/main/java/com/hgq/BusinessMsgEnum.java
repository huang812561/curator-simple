package com.hgq;

import lombok.Data;
import lombok.Getter;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @ClassName com.hgq.BusinessMsgEnum
 * @Description: TODO
 * @Author: hgq
 * @Date: 2021-09-24 17:03
 * @Version: 1.0
 */
public enum BusinessMsgEnum {
    SYSTEM_EXCEPTION("500", "系统异常"),
    ;

    BusinessMsgEnum(String code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    private String code;
    private String msg;

    public static String getValue(BusinessMsgEnum msgEnum) {
        Map<String,String> dataMap = Arrays.stream(BusinessMsgEnum.values()).collect(Collectors.toMap(BusinessMsgEnum::getCode, BusinessMsgEnum::getMsg, (key1, key2) -> key2));
        return dataMap.get(msgEnum.getCode());
    }


    public String getCode() {
        return code;
    }

    public String getMsg() {
        return msg;
    }
}
