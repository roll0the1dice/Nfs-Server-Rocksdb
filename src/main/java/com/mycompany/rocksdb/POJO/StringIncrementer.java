package com.mycompany.rocksdb.POJO;

public class StringIncrementer {

    /**
     * 对形如 "prefix-number" 的字符串，将其中的 number部分加一。
     *
     * @param input 原始字符串，例如 "xxxx-123"
     * @return      处理后的字符串，例如 "xxxx-124"
     * @throws IllegalArgumentException 如果输入格式不正确（没有'-'或'-'后不是数字）
     */
    public static String incrementNumberAfterHyphen(String input) {
        // 1. 检查输入是否为空
        if (input == null || input.isEmpty()) {
            throw new IllegalArgumentException("Input string cannot be null or empty.");
        }

        // 2. 找到最后一个连字符的位置
        int lastHyphenIndex = input.lastIndexOf('-');

        // 3. 检查是否找到了连字符
        if (lastHyphenIndex == -1) {
            throw new IllegalArgumentException("Input string does not contain a hyphen '-'.");
        }

        // 4. 分割字符串为前缀和数字部分
        String prefix = input.substring(0, lastHyphenIndex);
        String numberPart = input.substring(lastHyphenIndex + 1);

        try {
            // 5. 将数字部分转换为 long 类型并加一
            long number = Long.parseLong(numberPart);
            number++;

            // 6. 重新拼接字符串并返回
            return prefix + "-" + number;
        } catch (NumberFormatException e) {
            // 如果连字符后的部分不是有效数字，则抛出异常
            throw new IllegalArgumentException("The part after the hyphen is not a valid number: " + numberPart, e);
        }
    }

    public static void main(String[] args) {
        String originalString = "00000000000046857681750232228974-1000001";
        String resultString = incrementNumberAfterHyphen(originalString);

        System.out.println("Original: " + originalString);
        System.out.println("Result:   " + resultString);

        // 验证结果
        String expectedString = "00000000000046857681750232228974-1000002";
        System.out.println("Matches expected: " + resultString.equals(expectedString));

        // 测试其他例子
        System.out.println("\n--- Other examples ---");
        System.out.println("test-99 -> " + incrementNumberAfterHyphen("test-99"));
        System.out.println("file-part-0 -> " + incrementNumberAfterHyphen("file-part-0"));

        // 测试错误情况
        try {
            incrementNumberAfterHyphen("no-hyphen-here");
        } catch (IllegalArgumentException e) {
            // 这是一个不规范的例子，因为 'here' 不是数字，应该用下面的例子
        }
        try {
            incrementNumberAfterHyphen("no_hyphen_at_all");
        } catch (Exception e) {
            System.out.println("\nCaught expected error: " + e.getMessage());
        }
        try {
            incrementNumberAfterHyphen("prefix-notanumber");
        } catch (Exception e) {
            System.out.println("Caught expected error: " + e.getMessage());
        }
    }
}