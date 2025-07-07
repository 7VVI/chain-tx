package com.konors.chaintxcore.fluent;

import java.util.Objects;
import java.util.UUID;

/**
 * @author zhangyh
 * @Date 2025/7/7 10:48
 * @desc 类型安全的数据键，用于从异构数据容器中存取数据。
 *   每个实例都是唯一的。
 */
public final class DataKey<T> {
    private final String name;
    private final String uniqueId; // 保证每个key实例的唯一性

    private DataKey(String name) {
        this.name = name;
        this.uniqueId = UUID.randomUUID().toString();
    }

    /**
     * 创建一个新的数据键。
     * @param name 键的描述性名称，方便调试
     */
    public static <T> DataKey<T> of(String name) {
        return new DataKey<>(name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataKey<?> dataKey = (DataKey<?>) o;
        return uniqueId.equals(dataKey.uniqueId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uniqueId);
    }

    @Override
    public String toString() {
        return "DataKey{" + "name='" + name + '\'' + '}';
    }
}

