package com.konors.chaintxcore.support;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author zhangyh
 * @Date 2025/7/1 14:32
 * @desc 通用工作流上下文的基类。
 */
public abstract class WorkflowContext<S> {
    public final List<S> sourceItems;

    /** 存储预加载的查找数据, Key是缓存名称 (e.g., "countries") */
    final Map<String, Map<Object, Object>> lookups = new ConcurrentHashMap<>();

    /** 存储在内存中构建的实体, Key是实体类, Value是<源标识, 实体> */
    final Map<Class<?>, Map<Object, Object>> builtEntities = new ConcurrentHashMap<>();

    protected WorkflowContext(List<S> sourceItems) {
        this.sourceItems = sourceItems;
    }

    /**
     * 从上下文中获取预加载的查找数据。
     * @param cacheName 缓存名称
     * @param <T>       实体类型
     * @return 查找数据的Map
     */
    @SuppressWarnings("unchecked")
    public <T> Map<Object, T> getLookup(String cacheName) {
        return (Map<Object, T>) lookups.getOrDefault(cacheName, Collections.emptyMap());
    }
}

