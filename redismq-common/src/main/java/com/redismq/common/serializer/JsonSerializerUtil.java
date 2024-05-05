package com.redismq.common.serializer;

import org.apache.commons.lang3.StringUtils;

public class JsonSerializerUtil {
    public static boolean isJson(String bodyStr) {
        if (StringUtils.isBlank(bodyStr)) {
            return false;
        }
        return isWrap(bodyStr.trim(), '{', '}') || isWrap(bodyStr.trim(), '[', ']');
    }
    
    public static boolean isWrap(CharSequence str, char prefixChar, char suffixChar) {
        if (null == str) {
            return false;
        }
        
        return str.charAt(0) == prefixChar && str.charAt(str.length() - 1) == suffixChar;
    }
}
