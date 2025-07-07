package com.konors.chaintxcore.assembler;

import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @author zhangyh
 * @Date 2025/7/7 10:57
 * @desc
 */
public class DataAssembler<T> {

    private final DataProvider<T> dataProvider;

    private DataAssembler(DataProvider<T> dataProvider) {
        this.dataProvider = dataProvider;
    }

    public static <T> DataAssembler<T> source(DataProvider<T> provider) {
        return new DataAssembler<>(provider);
    }

    public static <T> DataAssembler<T> source(List<T> sourceData) {
        return new DataAssembler<>(() -> sourceData);
    }

    public <U> PairedDataAssembler<T, U> data(DataProvider<U> nextProvider) {
        return new PairedDataAssembler<>(this.dataProvider, nextProvider);
    }

    public <U> PairedDataAssembler<T, U> data(List<U> nextData) {
        return new PairedDataAssembler<>(this.dataProvider, () -> nextData);
    }

    // --- 流式操作 (懒加载) ---

    public DataAssembler<T> filter(Predicate<T> predicate) {
        return new DataAssembler<>(() -> {
            List<T> data = this.dataProvider.get();
            return data.stream().filter(predicate).collect(Collectors.toList());
        });
    }

    public <R> DataAssembler<R> map(Function<T, R> mapper) {
        return new DataAssembler<>(() -> {
            List<T> data = this.dataProvider.get();
            return data.stream().map(mapper).collect(Collectors.toList());
        });
    }

    public DataAssembler<T> sorted(Comparator<T> comparator) {
        return new DataAssembler<>(() -> {
            List<T> data = this.dataProvider.get();
            return data.stream().sorted(comparator).collect(Collectors.toList());
        });
    }

    public DataAssembler<T> distinct() {
        return new DataAssembler<>(() -> this.dataProvider.get().stream().distinct().collect(Collectors.toList()));
    }

    public DataAssembler<T> limit(long maxSize) {
        return new DataAssembler<>(() -> this.dataProvider.get().stream().limit(maxSize).collect(Collectors.toList()));
    }


    public void forEach(Consumer<T> action) {
        get().forEach(action);
    }

    public List<T> get() {
        return this.dataProvider.get();
    }
}
