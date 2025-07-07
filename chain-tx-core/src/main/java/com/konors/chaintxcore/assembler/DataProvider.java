package com.konors.chaintxcore.assembler;

import java.util.List;

/**
 * @author zhangyh
 * @Date 2025/7/7 10:57
 * @desc
 */
@FunctionalInterface
public interface DataProvider<T> {
    List<T> get();
}
