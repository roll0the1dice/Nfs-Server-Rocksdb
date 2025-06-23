package com.mycompany.rocksdb.utils;


import com.mycompany.rocksdb.enums.BaseEnum;
import com.mycompany.rocksdb.enums.Nfs3Procedure;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class EnumUtil {
  // 使用 ConcurrentHashMap 保证线程安全，并缓存不同枚举类型的查找Map
  private static Map<Class<? extends BaseEnum>, Map<Integer, BaseEnum>> map = new ConcurrentHashMap<Class<? extends BaseEnum>, Map<Integer, BaseEnum>>();

  public static <T extends Enum<T> & BaseEnum> T fromCode(Class<T> enumClass, int code) {
    Map<Integer, BaseEnum> enumMap = map.computeIfAbsent(enumClass, k -> {
      Map<Integer, BaseEnum> tMap = new HashMap<Integer, BaseEnum>();
      for (BaseEnum enumConstant : enumClass.getEnumConstants()) {
        tMap.put(enumConstant.getCode(), enumConstant);
      }

      return tMap;
    });

    return (T) enumMap.get(code);
  }
}
