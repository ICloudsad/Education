package com.lion.util;

import com.alibaba.fastjson.JSON;

/**
 * @ClassName: JSONUtil
 * @Author: Lion
 * @Date: 2023/5/1 20:06
 * @Description:
 */
public class JSONUtil {
    public static boolean isJSONValidate(String message){
        try {
            JSON.parseObject(message);
            return true;
        }catch (Exception e){
            return false;
        }
    }
}
