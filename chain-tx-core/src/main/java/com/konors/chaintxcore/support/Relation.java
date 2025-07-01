package com.konors.chaintxcore.support;

import lombok.Data;

import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * @author zhangyh
 * @Date 2025/7/1 14:28
 * @desc
 */
@Data
public class Relation<S, T> {

    private final BiConsumer<T, Object> idSetter;
    private final Class<?> foreignEntityClass;
    private final Function<S, Object> foreignKeyExtractor;
    private final String name;

    public Relation(BiConsumer<T, Object> idSetter,
                    Class<?> foreignEntityClass,
                    Function<S, Object> foreignKeyExtractor,
                    String name) {
        this.idSetter = idSetter;
        this.foreignEntityClass = foreignEntityClass;
        this.foreignKeyExtractor = foreignKeyExtractor;
        this.name = name;
    }
}
