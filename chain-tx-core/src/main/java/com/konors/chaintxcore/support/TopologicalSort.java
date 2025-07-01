package com.konors.chaintxcore.support;

import java.util.*;

/**
 * @author zhangyh
 * @Date 2025/7/1 14:30
 * @desc
 */
public class TopologicalSort {

    /**
     * 对实体类进行拓扑排序，以确定正确的持久化顺序。
     * @param nodes 待排序的实体类集合 (e.g., {Receiver.class, Mountpoint.class})
     * @param graph 依赖关系图
     * @return 按持久化顺序排序的实体类列表
     */
    public static List<Class<?>> sort(Set<Class<?>> nodes, Map<Class<?>, ? extends List<? extends Relation<?, ?>>> graph) {
        List<Class<?>> sortedList = new ArrayList<>();
        Map<Class<?>, Integer> inDegree = new HashMap<>();
        Map<Class<?>, List<Class<?>>> adjacencyList = new HashMap<>();

        // 初始化
        for (Class<?> node : nodes) {
            inDegree.put(node, 0);
            adjacencyList.put(node, new ArrayList<>());
        }

        // 构建邻接表和计算入度
        // 遍历时使用通配符类型
        for (Map.Entry<Class<?>, ? extends List<? extends Relation<?, ?>>> entry : graph.entrySet()) {
            Class<?> dependentNode = entry.getKey();
            if (!nodes.contains(dependentNode)) continue;

            for (Relation<?, ?> relation : entry.getValue()) {
                Class<?> dependencyNode = relation.getForeignEntityClass();
                if (!nodes.contains(dependencyNode)) continue;

                adjacencyList.get(dependencyNode).add(dependentNode);
                inDegree.put(dependentNode, inDegree.get(dependentNode) + 1);
            }
        }

        // 将所有入度为0的节点加入队列
        Queue<Class<?>> queue = new LinkedList<>();
        for (Map.Entry<Class<?>, Integer> entry : inDegree.entrySet()) {
            if (entry.getValue() == 0) {
                queue.offer(entry.getKey());
            }
        }

        // BFS过程
        while (!queue.isEmpty()) {
            Class<?> node = queue.poll();
            sortedList.add(node);

            for (Class<?> neighbor : adjacencyList.get(node)) {
                inDegree.put(neighbor, inDegree.get(neighbor) - 1);
                if (inDegree.get(neighbor) == 0) {
                    queue.offer(neighbor);
                }
            }
        }

        // 环路检测
        if (sortedList.size() != nodes.size()) {
            throw new IllegalStateException("A cyclic dependency was detected in the persistence graph.");
        }

        return sortedList;
    }
}
