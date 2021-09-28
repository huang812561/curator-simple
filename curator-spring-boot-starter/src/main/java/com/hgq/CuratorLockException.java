package com.hgq;

/**
 * @ClassName com.hgq.CuratorLockException
 * @Description: TODO
 * @Author: hgq
 * @Date: 2021-09-24 17:01
 * @Version: 1.0
 */
public class CuratorLockException extends RuntimeException {


    public CuratorLockException(BusinessMsgEnum msgEnum) {
        super(BusinessMsgEnum.getValue(msgEnum));
    }

    public CuratorLockException(Exception e){
        super(e);
    }
}
