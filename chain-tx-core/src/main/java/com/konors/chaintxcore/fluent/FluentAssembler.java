package com.konors.chaintxcore.fluent;

import org.springframework.beans.BeanUtils;

import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author zhangyh
 * @Date 2025/7/7 10:50
 * @desc 通用的、声明式的链式组装器 (Declarative Fluent Assembler)
 * @param <S> 源对象类型 (Source)
 * @param <T> 目标对象类型 (Target)
 */
public class FluentAssembler<S, T> {

    private final S source;
    private final T target;

    private FluentAssembler(S source, Supplier<T> targetFactory) {
        if (source == null) {
            throw new IllegalArgumentException("Source object cannot be null.");
        }
        this.source = source;
        this.target = targetFactory.get();
    }

    /**
     * 静态工厂方法，开始一个组装流程。
     *
     * @param source        源对象
     * @param targetFactory 目标对象的工厂 (例如：Vo::new)
     * @return a new FluentAssembler instance
     */
    public static <S, T> FluentAssembler<S, T> from(S source, Supplier<T> targetFactory) {
        return new FluentAssembler<>(source, targetFactory);
    }

    /**
     * 核心方法：为目标对象的指定字段设置一个值。
     *
     * @param setter        目标对象的setter方法引用 (e.g., Target::setField)
     * @param valueSupplier 提供值的Supplier (e.g., () -> source.getValue())
     * @param <V>           值的类型
     * @return this, for chaining
     */
    public <V> FluentAssembler<S, T> set(BiConsumer<T, V> setter, Supplier<V> valueSupplier) {
        setter.accept(this.target, valueSupplier.get());
        return this;
    }

    /**
     * `set` 方法的重载，简化从源对象直接获取值的场景。
     *
     * @param setter   目标对象的setter方法引用
     * @param getter   源对象的getter方法引用
     * @param <V>      值的类型
     * @return this, for chaining
     */
    public <V> FluentAssembler<S, T> set(BiConsumer<T, V> setter, Function<S, V> getter) {
        return set(setter, () -> getter.apply(this.source));
    }

    /**
     * 方便的快捷方法，用于执行基础属性复制。
     * 注意：这会覆盖所有已设置的同名属性。建议作为链式调用的第一步。
     *
     * @return this, for chaining
     */
    public FluentAssembler<S, T> copyProperties() {
        BeanUtils.copyProperties(this.source, this.target);
        return this;
    }

    /**
     * 完成组装，返回最终的目标对象。
     *
     * @return the fully assembled target object
     */
    public T get() {
        return this.target;
    }
}
