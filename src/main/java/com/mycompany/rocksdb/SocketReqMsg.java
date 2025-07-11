package com.mycompany.rocksdb;

import com.dslplatform.json.CompiledJson;
import com.dslplatform.json.JsonAttribute;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.HashMap;
import java.util.Map;

/**
 * SocketReqMsg
 *
 * @author liyixin
 * @date 2018/12/3
 */

@Data
@Accessors(chain = true)
public class SocketReqMsg {

    @JsonAttribute(nullable = false)
    public String version = "1.0";

    @JsonAttribute(nullable = false)
    public String msgType;

    @JsonAttribute(nullable = false)
    public long msgLen = 0;

    @JsonAttribute(nullable = false)
    public Map<String, String> dataMap;

    public SocketReqMsg() {
    }

    public SocketReqMsg(String msgType, long msgLen) {
        this.msgType = msgType;
        this.msgLen = msgLen;
        dataMap = new HashMap<>(16);
    }

    public SocketReqMsg put(String key, String value) {
        dataMap.put(key, value);
        return this;
    }

    public SocketReqMsg replace(String key, String oldValue, String newValue) {
        dataMap.replace(key, oldValue, newValue);
        return this;
    }

    public SocketReqMsg put(Map<String, String> map) {
        for (Map.Entry<String, String> entry : map.entrySet()) {
            dataMap.put(entry.getKey(), entry.getValue());
        }
        return this;
    }

    public String get(String key) {
        return dataMap.get(key);
    }

    public String getAndRemove(String key) {
        return dataMap.remove(key);
    }

    public SocketReqMsg copy() {
        SocketReqMsg copy = new SocketReqMsg(msgType, msgLen);
        copy.dataMap.putAll(dataMap);
        return copy;
    }
}
