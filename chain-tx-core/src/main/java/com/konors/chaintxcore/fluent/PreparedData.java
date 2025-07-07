package com.konors.chaintxcore.fluent;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author zhangyh
 * @Date 2025/7/7 10:49
 * @desc
 */
public class PreparedData {
    private final Map<DataKey<?>, Object> dataMap = new ConcurrentHashMap<>();

    <T> void put(DataKey<T> key, T value) {
        dataMap.put(key, value);
    }

    /**
     * 根据类型安全的键获取数据。
     * @param key 数据键
     * @return 查询到的数据
     * @throws IllegalStateException 如果键不存在
     */
    @SuppressWarnings("unchecked")
    public <T> T get(DataKey<T> key) {
        if (!dataMap.containsKey(key)) {
            throw new IllegalStateException("Data for key '" + key + "' was not prepared or failed to load.");
        }
        return (T) dataMap.get(key);
    }
}

