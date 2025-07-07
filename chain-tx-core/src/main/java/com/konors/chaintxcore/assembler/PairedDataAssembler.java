package com.konors.chaintxcore.assembler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
    private Function<T, ?>    keyExtractorT;
    private Function<U, ?> keyExtractorU;

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

    public <R> DataAssembler<R> assemble(BiFunction<T, U, R> assemblerFunction) {
        validateState();
        return  DataAssembler.source(() -> {
            if (isFastPathEnabled()) {
                return assembleWithFastPath(assemblerFunction);
            } else {
                return assembleWithSlowPath(assemblerFunction);
            }
        });
    }

    public <R> DataAssembler<R> leftAssemble(BiFunction<T, U, R> assemblerFunction) {
        validateState();
        return DataAssembler.source(() -> {
            if (isFastPathEnabled()) {
                return leftAssembleWithFastPath(assemblerFunction);
            } else {
                return leftAssembleWithSlowPath(assemblerFunction);
            }
        });
    }

    private <R> List<R> assembleWithFastPath(BiFunction<T, U, R> assemblerFunction) {
        List<T> currentData = providerT.get();
        List<U> nextData = providerU.get();
        Map<Object, List<U>> lookupMap = buildLookupMap(nextData);

        List<R> results = new ArrayList<>();
        for (T currentItem : currentData) {
            Object key = keyExtractorT.apply(currentItem);
            List<U> matchedItems = lookupMap.getOrDefault(key, Collections.emptyList());
            for (U matchedItem : matchedItems) {
                results.add(assemblerFunction.apply(currentItem, matchedItem));
            }
        }
        return results;
    }

    private <R> List<R> leftAssembleWithFastPath(BiFunction<T, U, R> assemblerFunction) {
        List<T> currentData = providerT.get();
        List<U> nextData = providerU.get();
        Map<Object, List<U>> lookupMap = buildLookupMap(nextData);

        List<R> results = new ArrayList<>();
        for (T currentItem : currentData) {
            Object key = keyExtractorT.apply(currentItem);
            List<U> matchedItems = lookupMap.getOrDefault(key, Collections.emptyList());
            if (matchedItems.isEmpty()) {
                results.add(assemblerFunction.apply(currentItem, null));
            } else {
                for (U matchedItem : matchedItems) {
                    results.add(assemblerFunction.apply(currentItem, matchedItem));
                }
            }
        }
        return results;
    }

    private <R> List<R> assembleWithSlowPath(BiFunction<T, U, R> assemblerFunction) {
        List<T> currentData = providerT.get();
        List<U> nextData = providerU.get();
        List<R> results = new ArrayList<>();
        for (T currentItem : currentData) {
            for (U nextItem : nextData) {
                if (slowMatchCondition.test(currentItem, nextItem)) {
                    results.add(assemblerFunction.apply(currentItem, nextItem));
                }
            }
        }
        return results;
    }

    private <R> List<R> leftAssembleWithSlowPath(BiFunction<T, U, R> assemblerFunction) {
        List<T> currentData = providerT.get();
        List<U> nextData = providerU.get();
        List<R> results = new ArrayList<>();
        for (T currentItem : currentData) {
            List<U> matchedItems = nextData.stream()
                    .filter(nextItem -> slowMatchCondition.test(currentItem, nextItem))
                    .collect(Collectors.toList());
            if (matchedItems.isEmpty()) {
                results.add(assemblerFunction.apply(currentItem, null));
            } else {
                for (U matchedItem : matchedItems) {
                    results.add(assemblerFunction.apply(currentItem, matchedItem));
                }
            }
        }
        return results;
    }

    @SuppressWarnings("unchecked")
    private Map<Object, List<U>> buildLookupMap(List<U> data) {
        Function<U, ?> extractor = this.keyExtractorU;
        return data.stream().collect(Collectors.groupingBy((Function<U, Object>) extractor));
    }

    private boolean isFastPathEnabled() {
        return keyExtractorT != null && keyExtractorU != null;
    }

    private void validateState() {
        if (!isFastPathEnabled() && slowMatchCondition == null) {
            throw new IllegalStateException("在调用 assemble（） 或左 Assemble（） 之前，必须调用 match（） 或 match On（）.");
        }
    }
}
