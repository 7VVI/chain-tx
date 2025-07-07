package com.konors.chaintxcore.fluent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * @author zhangyh
 * @Date 2025/7/7 10:49
 * @desc
 */
public class AsyncDataFetcher {

    private final Map<DataKey<?>, Supplier<?>> suppliers = new HashMap<>();
    private final Executor                     executor;

    /**
     * 使用默认的 ForkJoinPool.commonPool() 作为线程池。
     */
    public AsyncDataFetcher() {
        this(null);
    }

    /**
     * 使用指定的线程池。
     * @param executor 用于执行异步任务的线程池
     */
    public AsyncDataFetcher(Executor executor) {
        this.executor = executor;
    }

    /**
     * 注册一个异步数据提供者。
     *
     * @param name      一个描述性名称，用于创建DataKey
     * @param supplier  提供数据的Supplier，它将被异步执行
     * @return 一个类型安全的DataKey，用于后续获取数据
     */
    public <T> DataKey<T> register(String name, Supplier<T> supplier) {
        DataKey<T> key = DataKey.of(name);
        suppliers.put(key, supplier);
        return key;
    }

    /**
     * 执行所有已注册的异步查询，等待它们完成后，使用结果来组装对象列表。
     *
     * @param sourceList    源对象列表
     * @param assemblerFunc 组装函数，接收单个源对象和准备好的数据容器，返回组装好的目标对象
     * @param <S>           源类型
     * @param <T>           目标类型
     * @return 组装好的目标对象列表
     */
    public <S, T> List<T> executeAndAssemble(List<S> sourceList, BiFunction<S, PreparedData, T> assemblerFunc) {
        if (sourceList == null || sourceList.isEmpty()) {
            return new ArrayList<>();
        }

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        PreparedData preparedData = new PreparedData();

        // **修改点：用 for-each 循环替代 forEach lambda，并调用泛型辅助方法**
        for (Map.Entry<DataKey<?>, Supplier<?>> entry : suppliers.entrySet()) {
            CompletableFuture<Void> future = addFutureTask(entry.getKey(), entry.getValue(), preparedData);
            futures.add(future);
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        return sourceList.stream()
                .map(source -> assemblerFunc.apply(source, preparedData))
                .collect(Collectors.toList());
    }

    /**
     * **新增的私有泛型辅助方法**
     * 这个方法捕获了通配符 '?' 对应的具体类型 V。
     *
     * @param key          数据键
     * @param supplier     数据提供者
     * @param preparedData 数据容器
     * @param <V>          被捕获的具体类型
     * @return 一个 CompletableFuture<Void>，代表了异步任务的完成状态
     */
    @SuppressWarnings("unchecked")
    private <V> CompletableFuture<Void> addFutureTask(DataKey<?> key, Supplier<?> supplier, PreparedData preparedData) {
        // 在这里，我们将 Supplier<?> 安全地转换为 Supplier<V>
        Supplier<V> typedSupplier = (Supplier<V>) supplier;

        // 我们将 DataKey<?> 安全地转换为 DataKey<V>
        DataKey<V> typedKey = (DataKey<V>) key;

        return runAsync(typedSupplier, executor)
                .thenAccept(result -> preparedData.put(typedKey, result));
    }

    // 辅助方法，处理 Executor 为 null 的情况
    private <U> CompletableFuture<U> runAsync(Supplier<U> supplier, Executor executor) {
        if (executor != null) {
            return CompletableFuture.supplyAsync(supplier, executor);
        } else {
            return CompletableFuture.supplyAsync(supplier);
        }
    }
}

