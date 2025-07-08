package com.konors.chaintxcore.assembler;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author zhangyh
 * @Date 2025/7/7 10:58
 * @desc
 */
public class PairedDataAssembler<T, U> {

    private final DataProvider<T> providerT;
    private final DataProvider<U> providerU;

    private BiPredicate<T, U> slowMatchCondition;
    private Function<T, ?> keyExtractorT;
    private Function<U, ?> keyExtractorU;

    // 用于区分连接类型的内部枚举
    private enum JoinType {
        INNER, LEFT, RIGHT, FULL
    }

    PairedDataAssembler(DataProvider<T> providerT, DataProvider<U> providerU) {
        this.providerT = providerT;
        this.providerU = providerU;
    }

    public PairedDataAssembler<T, U> match(BiPredicate<T, U> condition) {
        this.slowMatchCondition = condition;
        this.keyExtractorT = null;
        this.keyExtractorU = null;
        return this;
    }

    public <K> PairedDataAssembler<T, U> matchOn(Function<T, K> keyExtractorT, Function<U, K> keyExtractorU) {
        this.keyExtractorT = keyExtractorT;
        this.keyExtractorU = keyExtractorU;
        this.slowMatchCondition = null;
        return this;
    }

    /**
     * 内连接：仅返回 T 和 U 能匹配上的数据。
     * @param assemblerFunction 组装函数，T 和 U 均不为 null。
     */
    public <R> DataAssembler<R> assemble(BiFunction<T, U, R> assemblerFunction) {
        return assembleInternal(JoinType.INNER, assemblerFunction);
    }

    /**
     * 左外连接：返回所有 T 的数据。如果 T 在 U 中有匹配，则 U 不为 null；否则 U 为 null。
     * @param assemblerFunction 组装函数，T 不为 null，U 可能为 null。
     */
    public <R> DataAssembler<R> leftAssemble(BiFunction<T, U, R> assemblerFunction) {
        return assembleInternal(JoinType.LEFT, assemblerFunction);
    }

    /**
     * 右外连接：返回所有 U 的数据。如果 U 在 T 中有匹配，则 T 不为 null；否则 T 为 null。
     * @param assemblerFunction 组装函数，U 不为 null，T 可能为 null。
     */
    public <R> DataAssembler<R> rightAssemble(BiFunction<T, U, R> assemblerFunction) {
        return assembleInternal(JoinType.RIGHT, assemblerFunction);
    }

    /**
     * 全外连接：返回所有 T 和 U 的数据。
     * 如果 T 和 U 匹配，则两者都不为 null。
     * 如果 T 无匹配，则 U 为 null。
     * 如果 U 无匹配，则 T 为 null。
     * @param assemblerFunction 组装函数，T 或 U 可能为 null（但不会同时为 null）。
     */
    public <R> DataAssembler<R> fullAssemble(BiFunction<T, U, R> assemblerFunction) {
        return assembleInternal(JoinType.FULL, assemblerFunction);
    }

    // --- 内部实现 ---

    private <R> DataAssembler<R> assembleInternal(JoinType joinType, BiFunction<T, U, R> assemblerFunction) {
        validateState();
        // 返回一个新的 DataAssembler，实现懒加载
        return DataAssembler.source(() -> {
            List<T> dataT = providerT.get();
            List<U> dataU = providerU.get();
            if (isFastPathEnabled()) {
                return assembleWithFastPath(joinType, dataT, dataU, assemblerFunction);
            } else {
                return assembleWithSlowPath(joinType, dataT, dataU, assemblerFunction);
            }
        });
    }

    @SuppressWarnings("unchecked")
    private <R> List<R> assembleWithFastPath(JoinType joinType, List<T> dataT, List<U> dataU, BiFunction<T, U, R> assembler) {
        List<R> results = new ArrayList<>();
        // 核心优化：构建U的查找表
        Map<Object, List<U>> lookupMapU = dataU.stream()
                .collect(Collectors.groupingBy((Function<U, Object>) this.keyExtractorU));

        Set<Object> usedUKeys = new HashSet<>();

        // 处理T侧的匹配 (适用于 INNER, LEFT, FULL)
        if (joinType == JoinType.INNER || joinType == JoinType.LEFT || joinType == JoinType.FULL) {
            for (T itemT : dataT) {
                Object key = keyExtractorT.apply(itemT);
                List<U> matchedItemsU = lookupMapU.get(key);

                if (matchedItemsU != null && !matchedItemsU.isEmpty()) {
                    usedUKeys.add(key);
                    for (U itemU : matchedItemsU) {
                        results.add(assembler.apply(itemT, itemU));
                    }
                } else if (joinType == JoinType.LEFT || joinType == JoinType.FULL) {
                    // T有但U没有匹配
                    results.add(assembler.apply(itemT, null));
                }
            }
        }

        // 处理U侧的匹配 (适用于 RIGHT, FULL)
        if (joinType == JoinType.RIGHT || joinType == JoinType.FULL) {
            // 如果是 RIGHT join, 我们需要构建T的查找表或者遍历U
            if (joinType == JoinType.RIGHT) {
                Map<Object, List<T>> lookupMapT = dataT.stream()
                        .collect(Collectors.groupingBy((Function<T, Object>) this.keyExtractorT));
                for (U itemU : dataU) {
                    Object key = keyExtractorU.apply(itemU);
                    List<T> matchedItemsT = lookupMapT.get(key);
                    if (matchedItemsT != null && !matchedItemsT.isEmpty()) {
                        for(T itemT : matchedItemsT) {
                            results.add(assembler.apply(itemT, itemU));
                        }
                    } else {
                        results.add(assembler.apply(null, itemU));
                    }
                }
            } else { // FULL join, 只处理未被使用过的U
                for(Map.Entry<Object, List<U>> entry : lookupMapU.entrySet()){
                    if(!usedUKeys.contains(entry.getKey())) {
                        for(U itemU : entry.getValue()) {
                            results.add(assembler.apply(null, itemU));
                        }
                    }
                }
            }
        }
        return results;
    }


    private <R> List<R> assembleWithSlowPath(JoinType joinType, List<T> dataT, List<U> dataU, BiFunction<T, U, R> assembler) {
        List<R> results = new ArrayList<>();
        Set<U> matchedUItems = new HashSet<>();
        Set<T> matchedTItems = new HashSet<>();

        // 先执行内连接逻辑，同时记录匹配项
        for (T itemT : dataT) {
            boolean tHasMatch = false;
            for (U itemU : dataU) {
                if (slowMatchCondition.test(itemT, itemU)) {
                    results.add(assembler.apply(itemT, itemU));
                    matchedTItems.add(itemT);
                    matchedUItems.add(itemU);
                    tHasMatch = true;
                }
            }
            // 如果是LEFT/FULL, 且T没有匹配上任何U
            if (!tHasMatch && (joinType == JoinType.LEFT || joinType == JoinType.FULL)) {
                results.add(assembler.apply(itemT, null));
            }
        }

        // 如果是INNER连接，到此结束
        if (joinType == JoinType.INNER) {
            return results;
        }

        // 处理RIGHT/FULL中，U有但T没有匹配上的情况
        if (joinType == JoinType.RIGHT || joinType == JoinType.FULL) {
            dataU.stream()
                    .filter(itemU -> !matchedUItems.contains(itemU))
                    .forEach(unmatchedU -> results.add(assembler.apply(null, unmatchedU)));
        }

        // 如果是RIGHT连接，结果只应包含匹配上的和未匹配的U，需过滤掉仅在T中存在的
        if (joinType == JoinType.RIGHT) {
            // 在上面的循环中，我们已经添加了所有与U匹配的T和所有未匹配的U。
            // 但这种实现方式略复杂，更好的方式是像FastPath一样分开处理。我们来重构一下。
            return assembleWithSlowPathRefactored(joinType, dataT, dataU, assembler);
        }

        return results;
    }

    // 慢速路径的更清晰实现
    private <R> List<R> assembleWithSlowPathRefactored(JoinType joinType, List<T> dataT, List<U> dataU, BiFunction<T, U, R> assembler) {
        List<R> results = new ArrayList<>();

        if (joinType == JoinType.INNER || joinType == JoinType.LEFT || joinType == JoinType.FULL) {
            Set<U> allMatchedU = new HashSet<>();
            for (T itemT : dataT) {
                List<U> matchesForT = dataU.stream()
                        .filter(itemU -> slowMatchCondition.test(itemT, itemU))
                        .collect(Collectors.toList());

                if (!matchesForT.isEmpty()) {
                    allMatchedU.addAll(matchesForT);
                    for (U itemU : matchesForT) {
                        results.add(assembler.apply(itemT, itemU));
                    }
                } else if (joinType == JoinType.LEFT || joinType == JoinType.FULL) {
                    results.add(assembler.apply(itemT, null));
                }
            }
            // 对于 FULL 连接，需要补充未匹配的 U
            if (joinType == JoinType.FULL) {
                dataU.stream()
                        .filter(itemU -> !allMatchedU.contains(itemU))
                        .forEach(unmatchedU -> results.add(assembler.apply(null, unmatchedU)));
            }
        }

        if (joinType == JoinType.RIGHT) {
            Set<T> allMatchedT = new HashSet<>();
            for (U itemU : dataU) {
                List<T> matchesForU = dataT.stream()
                        .filter(itemT -> slowMatchCondition.test(itemT, itemU))
                        .collect(Collectors.toList());

                if(!matchesForU.isEmpty()) {
                    allMatchedT.addAll(matchesForU);
                    for(T itemT : matchesForU) {
                        results.add(assembler.apply(itemT, itemU));
                    }
                } else {
                    results.add(assembler.apply(null, itemU));
                }
            }
        }

        // 由于INNER连接的逻辑在LEFT的循环中已完整覆盖，所以直接返回
        return results;
    }


    private boolean isFastPathEnabled() {
        return keyExtractorT != null && keyExtractorU != null;
    }

    private void validateState() {
        if (!isFastPathEnabled() && slowMatchCondition == null) {
            throw new IllegalStateException("Must call match() or matchOn() before calling an assemble method.");
        }
    }
}
