package com.konors.chaintxcore.support;

import com.konors.chaintxcore.exception.ChainTxRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author zhangyh
 * @Date 2025/7/1 14:32
 * @desc
 */
public class WorkflowEngine<S, C extends WorkflowContext<S>> {

    private static final Logger log = LoggerFactory.getLogger(WorkflowEngine.class);

    /**
     * 工作流的中央上下文对象。
     * 它在整个工作流的生命周期中传递，用于存储和共享所有中间状态，
     * 包括源数据、预加载的查找数据（字典）以及在内存中构建的实体。
     *
     * @see WorkflowContext
     */
    private final C context;

    /**
     * 预构建步骤列表。
     * 这些步骤在主构建阶段（buildSteps）之前执行。
     * 主要用于执行预加载操作，例如从数据库中获取所有涉及的国家、部门等“字典”数据，
     * 以便在后续的构建步骤中可以快速查找，避免N+1查询问题。
     * 通过 `.lookup()` 方法添加。
     */
    private final List<WorkflowStep<C>> preBuildSteps = new ArrayList<>();

    /**
     * 主构建步骤列表。
     * 这些步骤负责将源数据项（类型为 S）转换为内存中的实体对象。
     * 每个步骤通常对应一类实体的构建逻辑。例如，一个步骤用于构建Receiver，另一个用于构建Mountpoint。
     * 在这个阶段，实体对象只在内存中创建，不涉及数据库保存，也不处理ID关联。
     * 通过 `.build()` 方法添加。
     */
    private final List<WorkflowStep<C>> buildSteps = new ArrayList<>();

    /**
     * 持久化处理器（Saver）的注册表。
     * - Key:   实体类的 `Class` 对象 (e.g., `Receiver.class`)。
     * - Value: 一个 `Consumer`，它知道如何将一个特定类型的实体列表批量保存到数据库。
     * 通常，这个Consumer会调用相应的Repository的 `saveAll` 方法。
     * 使用 `Consumer<? extends List<?>>` 是为了处理泛型兼容性问题。
     * 引擎使用这个Map来找到并调用正确的保存逻辑。
     * 通过 `.persist()` 方法注册。
     */
    private final Map<Class<?>, Consumer<? extends List<?>>> persistSavers = new LinkedHashMap<>();

    /**
     * 源数据键提取器 (Source Key Extractor) 的注册表。
     * - Key:   实体类的 `Class` 对象 (e.g., `Receiver.class`)。
     * - Value: 一个函数，它知道如何从一条源数据（类型为 S）中提取用于唯一标识该实体的键。
     * 例如，对于Receiver，这个键可能是 `receiverCode`。
     * 这个Map至关重要，它建立了从“源数据”到“内存中构建的实体”之间的桥梁，
     * 使得框架能够在后续的ID回填步骤中精确地找到对应的实例。
     * 通过 `.build()` 方法的第二个参数注册。
     */
    private final Map<Class<?>, Function<S, Object>> sourceKeyExtractors = new HashMap<>();

    /**
     * 实体关系图 (Relation Graph)。
     * 这是整个框架实现自动依赖分析和ID回填的核心数据结构。
     * - Key:   子实体（依赖方）的 `Class` 对象 (e.g., `Mountpoint.class`)。
     * - Value: 一个 `Relation` 列表，描述了这个子实体所依赖的所有父实体（被依赖方）。
     * 每个 `Relation` 对象包含了设置外键ID的setter方法、父实体的Class对象以及如何从源数据中找到父实体Key的逻辑。
     * 拓扑排序算法会分析这个图，以确定正确的持久化顺序。
     * 通过 `.relate()` 方法构建。
     */
    private final Map<Class<?>, List<Relation<S, ?>>> relationGraph = new HashMap<>();

    /**
     * 持久化后ID获取器 (ID Getter) 的注册表。
     * - Key:   实体类的 `Class` 对象 (e.g., `Receiver.class`)。
     * - Value: 一个函数，它知道如何从一个已持久化的实体对象中获取其数据库生成的主键ID。
     * 通常，这是一个方法引用，如 `Receiver::getId`。
     * 这个Map是自动ID回填的关键，框架在持久化一个实体后，会使用对应的Getter获取其ID，
     * 然后根据关系图找到所有依赖它的子实体，并设置它们的外键。
     * 通过 `.persist()` 方法的第三个参数注册。
     */
    private final Map<Class<?>, Function<?, ?>> persistIdGetters = new HashMap<>();

    /**
     * 私有构造函数，防止外部直接实例化。应通过静态工厂方法 `create` 创建。
     *
     * @param context 为此次工作流实例化的上下文对象。
     */
    private WorkflowEngine(C context) {
        this.context = context;
    }

    /**
     * [静态工厂方法] 创建并启动一个新的工作流。
     * 这是所有链式调用的起点。
     *
     * @param sourceItems    源数据列表，是整个工作流的处理对象。
     * @param contextFactory 一个函数，用于根据源数据列表创建特定于业务的上下文实例。
     * @param <S>            源数据类型。
     * @param <C>            上下文类型。
     * @return 一个新的 {@link WorkflowEngine} 实例，准备好接收后续的步骤声明。
     */
    public static <S, C extends WorkflowContext<S>> WorkflowEngine<S, C> create(
            List<S> sourceItems, Function<List<S>, C> contextFactory) {
        return new WorkflowEngine<>(contextFactory.apply(sourceItems));
    }

    /**
     * [声明步骤] 预加载查找数据（“字典”数据）。
     * 此步骤会在所有 `build` 步骤之前执行。它从源数据中提取所有需要查询的键，
     * 然后批量地从数据库中获取这些键对应的实体，并将其存入上下文中以备后用。
     * 这可以有效避免在构建实体时发生N+1查询问题。
     *
     * @param cacheName       为这批查找数据指定的缓存名称，用于后续在上下文中通过 `context.getLookup(cacheName)` 获取。
     * @param keysExtractor   一个函数，用于从整个源数据集合中提取出所有唯一的、需要查询的键。
     * @param fetcher         一个函数，它接收一批键，并从数据库或其他数据源返回对应的实体列表。
     * @param entityKeyGetter 一个函数，用于从查询返回的实体中提取出其自身的键，以便构建成Map。
     * @param <K>             键的类型。
     * @param <T>             查找的实体类型。
     * @return 当前的 {@link WorkflowEngine} 实例，以支持链式调用。
     */
    public <K, T> WorkflowEngine<S, C> lookup(
            String cacheName, Function<Collection<S>, Set<K>> keysExtractor,
            Function<Set<K>, List<T>> fetcher, Function<T, K> entityKeyGetter) {
        preBuildSteps.add(ctx -> {
            Set<K> keys = keysExtractor.apply(ctx.sourceItems);
            if (keys == null || keys.isEmpty()) return;
            List<T> entities = fetcher.apply(keys);
            Map<Object, Object> lookupMap = entities.stream()
                    .collect(Collectors.toMap(entityKeyGetter, Function.identity(), (a, b) -> a));
            ctx.lookups.put(cacheName, lookupMap);
        });
        return this;
    }

    /**
     * [声明步骤] 定义如何根据源数据构建一个实体。
     * 此步骤将一个源数据项 `S` 转换为一个内存中的实体对象 `T`。
     * 它还会注册一个“源键提取器”，用于建立源数据与构建的实体之间的映射关系。
     *
     * @param entityClass        要构建的实体的 `Class` 对象。
     * @param sourceKeyExtractor 一个函数，用于从源数据项 `S` 中提取一个唯一的标识符（“源键”）。
     * @param builder            一个函数，它接收一个源数据项 `S` 和当前上下文 `C`，并返回构建好的实体对象 `T`。
     * @param <T>                要构建的实体类型。
     * @return 当前的 {@link WorkflowEngine} 实例。
     */
    public <T> WorkflowEngine<S, C> build(
            Class<T> entityClass, Function<S, Object> sourceKeyExtractor, BiFunction<S, C, T> builder) {
        this.sourceKeyExtractors.put(entityClass, sourceKeyExtractor);
        buildSteps.add(ctx -> {
            Map<Object, Object> entityMap = ctx.builtEntities.computeIfAbsent(entityClass, k -> new LinkedHashMap<>());
            for (S item : ctx.sourceItems) {
                Object key = sourceKeyExtractor.apply(item);
                if (key != null && !entityMap.containsKey(key)) {
                    T entity = builder.apply(item, ctx);
                    if (entity != null) {
                        entityMap.put(key, entity);
                    }
                }
            }
        });
        return this;
    }

    /**
     * [声明步骤] 定义实体之间的关联关系。
     * 这是实现自动ID回填的核心声明。它告诉引擎，一个实体（子）依赖于另一个实体（父）。
     *
     * @param entityClass         拥有外键的子实体 `Class` 对象。
     * @param idSetter            一个 `BiConsumer` (通常是方法引用，如 `Mountpoint::setReceiverId`)，用于将父实体的ID设置到子实体上。
     * @param foreignEntityClass  被依赖的父实体 `Class` 对象。
     * @param foreignKeyExtractor 一个函数，用于从源数据项 `S` 中提取出父实体的“源键”，以便框架能够找到正确的父实体实例。
     * @param relationName        一个描述性的名称，用于调试和日志记录。
     * @param <T>                 子实体类型。
     * @param <F>                 父实体类型。
     * @return 当前的 {@link WorkflowEngine} 实例。
     */
    public <T, F, I> WorkflowEngine<S, C> relate(
            Class<T> entityClass, BiConsumer<T, I> idSetter, Class<F> foreignEntityClass,
            Function<S, Object> foreignKeyExtractor, String relationName) {
        BiConsumer<T, Object> genericIdSetter = (entity, id) -> idSetter.accept(entity, (I) id);
        relationGraph.computeIfAbsent(entityClass, k -> new ArrayList<>())
                .add(new Relation<>(genericIdSetter, foreignEntityClass, foreignKeyExtractor, relationName));
        return this;
    }

    /**
     * [声明步骤] 定义如何持久化一类实体。
     *
     * @param entityClass 要持久化的实体的 `Class` 对象。
     * @param saver       一个 `Consumer` (通常是方法引用，如 `repository::saveAll`)，它接收一个实体列表并将其保存到数据库。
     * @param idGetter    一个函数 (通常是方法引用，如 `Receiver::getId`)，用于从已持久化的实体中获取其数据库生成的ID。
     * @param <T>         要持久化的实体类型。
     * @return 当前的 {@link WorkflowEngine} 实例。
     */
    public <T> WorkflowEngine<S, C> persist(Class<T> entityClass, Consumer<List<T>> saver, Function<T, Long> idGetter) {
        persistSavers.put(entityClass, saver);
        persistIdGetters.put(entityClass, idGetter);
        return this;
    }

    /**
     * [终端操作] 在一个数据库事务中执行整个工作流。
     * 如果工作流中的任何步骤抛出异常，事务将回滚。
     * 这是推荐的执行方式，以保证数据一致性。
     *
     * @param template Spring 的 {@link TransactionTemplate} 实例。
     */
    public void execute(TransactionTemplate template) {
        log.debug("Executing workflow within a transaction...");
        template.execute(status -> {
            try {
                runWorkflowSteps();
            } catch (Exception e) {
                e.printStackTrace();
                status.setRollbackOnly();
                throw new ChainTxRuntimeException("Workflow execution failed within transaction", e);
            }
            return null;
        });
    }

    /**
     * [终端操作] 执行整个工作流，但不使用外部事务管理器。
     * 适用于不需要事务保证的场景，例如操作非事务性数据源（如某些NoSQL数据库）或执行只读的分析流程。
     * 注意：如果在此过程中发生错误，已执行的数据库操作将不会被回滚。
     */
    public void executeWithoutTransaction() {
        log.debug("Executing workflow without a transaction...");
        try {
            runWorkflowSteps();
        } catch (Exception e) {
            e.printStackTrace();
            throw new ChainTxRuntimeException("Workflow execution failed", e);
        }
    }

    /**
     * 私有核心方法，包含了工作流的主要执行逻辑。
     *
     * @throws Exception 如果任何步骤失败，则抛出异常。
     */
    private void runWorkflowSteps() throws Exception {
        for (WorkflowStep<C> step : preBuildSteps) step.execute(context);
        for (WorkflowStep<C> step : buildSteps) step.execute(context);

        List<Class<?>> persistOrder = TopologicalSort.sort(persistSavers.keySet(), relationGraph);
        log.debug("Executing persistence steps in order: " + persistOrder.stream().map(Class::getSimpleName).collect(Collectors.toList()));
        for (Class<?> entityClass : persistOrder) {
            fillForeignKeyIds(entityClass);

            Map<Object, Object> entitiesToPersist = context.builtEntities.get(entityClass);
            if (entitiesToPersist == null || entitiesToPersist.isEmpty()) continue;

            List<?> entityList = new ArrayList<>(entitiesToPersist.values());
            Consumer<List<?>> saver = (Consumer<List<?>>) persistSavers.get(entityClass);
            log.debug("Persisting {} entities of type {}", entityList.size(), entityClass.getSimpleName());
            saver.accept(entityList);
            log.debug("Persisted {} entities of type {}", entityList.size(), entityClass.getSimpleName());
        }
    }

    /**
     * 私有核心方法，负责自动ID回填。
     * 对于给定的实体类型，它会查找其所有依赖关系，并从已经持久化的父实体中获取ID，
     * 然后设置到当前（尚未持久化的）实体实例上。
     *
     * @param dependentClass 需要被设置外键的实体类。
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void fillForeignKeyIds(Class<?> dependentClass) {
        List<Relation<S, ?>> relations = relationGraph.get(dependentClass);
        if (relations == null) return;

        Map<Object, Object> dependents = context.builtEntities.get(dependentClass);
        if (dependents == null) return;

        // 创建一个从源数据key到源数据项的映射，方便查找
        Function<S, Object> dependentKeyExtractor = sourceKeyExtractors.get(dependentClass);
        if (dependentKeyExtractor == null) {
            throw new IllegalStateException("No source key extractor registered for class: " + dependentClass.getName());
        }
        Map<Object, S> sourceKeyToItemMap = context.sourceItems.stream()
                .collect(Collectors.toMap(dependentKeyExtractor, Function.identity(), (a, b) -> a));

        for (Relation relation : relations) {
            Class<?> parentClass = relation.getForeignEntityClass();
            Map<Object, Object> parents = context.builtEntities.get(parentClass);
            if (parents == null) continue;

            Function<Object, Long> parentIdGetter = (Function<Object, Long>) persistIdGetters.get(parentClass);
            if (parentIdGetter == null) continue; // 父实体尚未持久化，其ID不可用

            dependents.forEach((dependentKey, dependentEntity) -> {
                S sourceItem = sourceKeyToItemMap.get(dependentKey);
                if (sourceItem == null) return;

                Object parentKey = relation.getForeignKeyExtractor().apply(sourceItem);
                if (parentKey == null) return;

                Object parentEntity = parents.get(parentKey);
                if (parentEntity != null) {
                    Long parentId = parentIdGetter.apply(parentEntity);
                    if (parentId != null) {
                        relation.getIdSetter().accept(dependentEntity, parentId);
                        log.debug("  -> Back-filling ID {} from {} (key: {}) to {} (key: {}) via relation '{}' ", parentId, parentClass.getSimpleName(), parentKey,
                                dependentClass.getSimpleName(), dependentKey, relation.getName());
                    }
                }
            });
        }
    }
}