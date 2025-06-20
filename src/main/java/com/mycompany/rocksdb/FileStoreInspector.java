package com.mycompany.rocksdb;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileStore;
import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FileStoreInspector {

    public static void main(String[] args) {
        System.out.println("--- Inspecting All Fields of FileStore Instances ---");

        if (System.getProperty("os.name").toLowerCase().contains("win")) {
            System.out.println("The internal structure of FileStore on Windows is different and less informative for mount points.");
        }

        try {
            for (FileStore store : FileSystems.getDefault().getFileStores()) {
                System.out.println("\n=======================================================================");
                System.out.println("Inspecting FileStore: " + store.name() + " (Type: " + store.type() + ")");
                System.out.println("Actual Class: " + store.getClass().getName());
                System.out.println("=======================================================================");

                inspectObjectFields(store);
            }
        } catch (Exception e) {
            System.err.println("An error occurred during inspection.");
            e.printStackTrace();
        }
    }

    /**
     * 使用反射遍历并打印一个对象及其所有父类的所有字段。
     * @param obj 要检查的对象
     */
    public static void inspectObjectFields(Object obj) {
        List<Field> fields = getAllFields(obj.getClass());

        System.out.println("--- Discovered Fields (including inherited) ---");
        for (Field field : fields) {
            try {
                // 强制访问私有字段
                field.setAccessible(true);

                // 获取字段信息
                String fieldName = field.getName();
                String fieldType = field.getType().getSimpleName();
                Object fieldValue = field.get(obj);
                String fieldValueStr;

                // 对特殊类型进行友好格式化
                if (fieldValue == null) {
                    fieldValueStr = "null";
                } else if (fieldValue instanceof byte[]) {
                    // 对 byte[] 进行特殊处理，避免打印 [B@hashcode
                    fieldValueStr = new String((byte[]) fieldValue, StandardCharsets.UTF_8).trim();
                    fieldValueStr = "byte[] as String: \"" + fieldValueStr + "\"";
                } else {
                    fieldValueStr = fieldValue.toString();
                }

                // 打印字段所属的类，以便区分
                String declaringClass = field.getDeclaringClass().getSimpleName();
                System.out.printf("  [%-20s] %-25s (%-15s) = %s%n",
                        declaringClass, fieldName, fieldType, fieldValueStr);

            } catch (Exception e) {
                System.err.printf("  Could not access field %s: %s%n", field.getName(), e.getMessage());
            }
        }
    }

    /**
     * 递归获取一个类及其所有父类的所有字段。
     * @param clazz 起始类
     * @return 包含所有字段的列表
     */
    public static List<Field> getAllFields(Class<?> clazz) {
        List<Field> fields = new ArrayList<>();
        while (clazz != null && clazz != Object.class) {
            fields.addAll(Arrays.asList(clazz.getDeclaredFields()));
            clazz = clazz.getSuperclass();
        }
        return fields;
    }
}
