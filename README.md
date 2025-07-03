# WorkflowEngine: 声明式事务性工作流引擎

![Java](https://img.shields.io/badge/Java-17+-orange.svg)
![Spring](https://img.shields.io/badge/Spring-Framework-green.svg)
![License](https://img.shields.io/badge/License-MIT-blue.svg)

**WorkflowEngine** 是一个轻量级、通用的Java工作流框架，旨在将复杂、易错的批量数据导入和处理逻辑，转化为简洁、可维护的声明式代码。它彻底解决了在处理具有复杂依赖关系的实体时，手动管理状态、持久化顺序和外键ID回填的痛点。

## 痛点：那个熟悉的“数据导入”噩梦

你是否也曾编写过这样的数据导入代码？

```java
public void importData(List<ExcelRow> rows) {
    // 1. 定义一大堆Map来维护中间状态
    Map<String, User> userMap = new HashMap<>();
    Map<String, Department> deptMap = new HashMap<>();
    Map<Order, String> orderToUserKey = new HashMap<>();
    Map<Order, String> orderToDeptKey = new HashMap<>();
    
    // 2. 提前查询所有可能用到的“字典”数据
    Set<String> userNames = rows.stream().map(...).collect(Collectors.toSet());
    List<User> existingUsers = userRepository.findByNames(userNames);
    existingUsers.forEach(u -> userMap.put(u.getName(), u));
    
    // 3. 遍历数据，进行大量的if-else判断和手动关联
    for (ExcelRow row : rows) {
        // 查找或创建User
        User user = userMap.get(row.getUserName());
        if (user == null) {
            user = new User(row.getUserName());
            // userRepository.save(user); // 糟糕！事务管理混乱
            userMap.put(user.getName(), user);
        }
        
        // 创建Order实体
        Order order = new Order(...);
        
        // 4. “ID回填地狱”：手动记录关系，等User保存后才能回填ID
        orderToUserKey.put(order, user.getName());
    }
    
    // 5. 在一个巨大的try-catch块中，手动控制保存顺序
    // transaction.begin();
    // userRepository.saveAll(newlyCreatedUsers);
    // for (Order order : orders) {
    //     User user = userMap.get(orderToUserKey.get(order));
    //     order.setUserId(user.getId()); // 手动回填ID
    // }
    // orderRepository.saveAll(orders);
    // transaction.commit();
}
```

这种过程式的代码存在诸多问题：
*   **状态爆炸**: 代码充斥着大量用于追踪状态的 `Map` 和 `Set`，难以阅读和维护。
*   **逻辑耦合**: 数据校验、转换、构建和持久化逻辑紧密耦合，违反了单一职责原则。
*   **顺序硬编码**: 持久化的顺序被硬编码在代码中。如果新增一个依赖，你需要手动调整这个顺序。
*   **事务管理困难**: 整个流程需要原子性，但复杂的逻辑使得事务边界模糊，容易出错。
*   **ID回填地狱**: 手动处理新插入实体的ID并回填到依赖它的实体中，是极其繁琐且容易出错的根源。

## 解决方案：用声明式工作流告别混乱

**WorkflowEngine** 将上述过程彻底颠覆。你不再需要告诉框架**如何**去做，只需要**声明你想要什么**。

想象一下，同样的功能，用WorkflowEngine来实现：

```java
public void importData(List<ExcelRow> rows) {
    WorkflowEngine.create(rows, ExcelContext::new)
        // 1. 声明：如何构建实体
        .build(User.class, ExcelRow::getUserName, (row, ctx) -> new User(row.getUserName()))
        .build(Order.class, ExcelRow::getOrderNumber, (row, ctx) -> new Order(row.getOrderNumber()))

        // 2. 声明：实体间的关联关系
        .relate(Order.class, Order::setUserId, User.class, 
                ExcelRow::getUserName, "Order-User-Relation")

        // 3. 声明：如何持久化实体
        .persist(User.class, userRepository::saveAll, User::getId)
        .persist(Order.class, orderRepository::saveAll, Order::getId)

        // 4. 执行！框架会处理剩下的一切
        .execute(transactionTemplate);
}
```

框架在幕后为你做了什么？
*   **自动状态管理**: 框架在内部的`Context`中自动管理所有构建的实体实例。
*   **自动依赖分析**: 通过分析你声明的 `.relate()` 关系，它使用**拓扑排序**算法自动计算出正确的持久化顺序 (`User` -> `Order`)。
*   **自动ID回填**: 在持久化 `User` 后，框架会立即获取其生成的ID，并自动调用 `Order::setUserId` 方法，将ID设置到所有关联的 `Order` 实例上，然后再持久化 `Order`。
*   **事务保证**: 所有数据库操作都在你提供的 `TransactionTemplate` 中原子性地执行。

## 核心功能

*   **声明式API**: 使用流畅的链式调用定义工作流，代码即文档。
*   **类型安全**: 充分利用Java泛型，在编译期捕获大部分错误。
*   **自动拓扑排序**: 无需手动关心实体持久化顺序。
*   **自动ID回填**: 彻底摆脱手动设置外键ID的繁琐工作。
*   **支持任意ID类型**: 不限于`Long`，支持`String` (UUID)、`Integer`等任意类型的主键。
*   **预加载支持**: 通过 `.lookup()` 方法避免N+1查询问题。
*   **事务集成**: 与Spring `TransactionTemplate` 无缝集成，保证数据一致性。
*   **高度可扩展**: 框架本身是通用的，你可以为任何业务场景创建特定的工作流。

## 使用指南

使用WorkflowEngine只需简单的四步：**构建、关联、持久化、执行**。

### 第1步：准备工作

#### 1.1 定义源数据DTO和实体
```java
// 源数据DTO (e.g., from Excel)
record ExcelRow(String orderNo, String customerName, Integer productCode) {}

// 实体 (支持任意ID类型)
class Customer {
    private String id; // String ID
    private String name;
    // getters and setters...
}

class Product {
    private Integer id; // Integer ID
    private Integer code;
    // getters and setters...
}

class SalesOrder {
    private String id;
    private String customerId; // Foreign Key
    private Integer productId; // Foreign Key
    // getters and setters...
}
```

#### 1.2 创建一个自定义上下文 (可选，但推荐)
```java
// 上下文用于在步骤间传递额外信息
class OrderContext extends WorkflowContext<ExcelRow> {
    public OrderContext(List<ExcelRow> sourceItems) {
        super(sourceItems);
    }
}
```

### 第2步：构建工作流

在一个Service方法中，开始构建你的工作流。

```java
@Service
public class OrderImportService {

    // 注入你的Repositories和TransactionTemplate
    @Autowired private CustomerRepository customerRepo;
    @Autowired private ProductRepository productRepo;
    @Autowired private SalesOrderRepository orderRepo;
    @Autowired private TransactionTemplate transactionTemplate;

    public void importOrders(List<ExcelRow> excelData) {
        WorkflowEngine.create(excelData, OrderContext::new)
            // ... 在这里定义你的步骤 ...
            .execute(transactionTemplate);
    }
}
```

### 第3步：声明工作流步骤

#### `.build(entityClass, sourceKeyExtractor, builder)`
告诉引擎如何根据一行源数据构建一个实体。

*   `entityClass`: 要构建的实体类，如 `Customer.class`。
*   `sourceKeyExtractor`: 一个函数，用于从源数据中提取一个唯一标识符，如 `ExcelRow::customerName`。框架用它来对实体进行去重。
*   `builder`: 一个函数，负责创建实体实例。

```java
.build(Customer.class, 
       ExcelRow::customerName, 
       (row, ctx) -> new Customer(row.customerName()))

.build(Product.class, 
       ExcelRow::productCode, 
       (row, ctx) -> new Product(row.productCode()))

.build(SalesOrder.class, 
       ExcelRow::orderNo, 
       (row, ctx) -> new SalesOrder(row.orderNo()))
```

#### `.relate(entityClass, idSetter, foreignEntityClass, foreignKeyExtractor, relationName)`
声明实体间的依赖关系，这是自动ID回填的关键。

*   `entityClass`: 子实体类 (e.g., `SalesOrder.class`)。
*   `idSetter`: 子实体的外键setter方法 (e.g., `SalesOrder::setCustomerId`)。
*   `foreignEntityClass`: 父实体类 (e.g., `Customer.class`)。
*   `foreignKeyExtractor`: 一个函数，用于从源数据中找到关联的父实体的Key (e.g., `ExcelRow::customerName`)。
*   `relationName`: 一个用于调试的描述性名称。

```java
.relate(SalesOrder.class, SalesOrder::setCustomerId, Customer.class,
        ExcelRow::customerName, "Order-Customer")

.relate(SalesOrder.class, SalesOrder::setProductId, Product.class,
        ExcelRow::productCode, "Order-Product")
```

#### `.persist(entityClass, saver, idGetter)`
告诉引擎如何持久化实体以及如何获取其ID。

*   `entityClass`: 要持久化的实体类。
*   `saver`: 保存实体列表的函数 (e.g., `customerRepo::saveAll`)。
*   `idGetter`: 获取实体ID的函数 (e.g., `Customer::getId`)。

```java
.persist(Customer.class, customerRepo::saveAll, Customer::getId)
.persist(Product.class, productRepo::saveAll, Product::getId)
.persist(SalesOrder.class, orderRepo::saveAll, SalesOrder::getId)
```

### 第4步：执行工作流

调用终端操作来触发整个流程。

#### `.execute(transactionTemplate)`
在数据库事务中执行所有步骤。**这是推荐的方式。**

```java
.execute(transactionTemplate);
```

#### `.executeWithoutTransaction()`
执行所有步骤，但不使用事务。适用于非事务性操作或简单脚本。

```java
.executeWithoutTransaction();
```

###  高级功能：使用 .lookup() 避免 N+1 查询

在数据导入场景中，我们经常需要处理一种“查找或创建”（Find or Create）的逻辑。例如，导入订单时，如果订单关联的客户已经存在于数据库中，我们应该使用现有的客户；如果不存在，则需要创建一个新的。

一个常见的错误做法是在循环中逐个查询数据库，这会导致 **N+1 查询问题**，严重影响性能。

**WorkflowEngine** 提供了 .lookup() 方法，旨在通过一次批量查询来优雅地解决这个问题。

#### .lookup() 的工作原理

.lookup() 步骤会在所有 .build() 步骤**之前**执行。它的工作流程如下：

1. **提取所有键**: 遍历所有源数据，提取出需要查询的所有唯一键（例如，所有客户名称）。
2. **一次性批量查询**: 使用这些键，通过一次数据库查询（如 SELECT * FROM customers WHERE name IN (...)）获取所有已存在的实体。
3. **缓存结果**: 将查询结果存入一个内部的 Map (我们称之为“查找缓存”或“字典”)，以便在后续的 .build() 步骤中可以快速、无IO地访问。

#### .lookup(cacheName, keysExtractor, fetcher, entityKeyGetter)

- cacheName: 一个字符串，作为这批查找数据的唯一标识。例如 "customers"。
- keysExtractor: 一个函数，负责从**整个源数据列表**中提取出所有需要查询的唯一键集合 (Set<K>)。
- fetcher: 一个函数，接收上一步提取的键集合，并从数据库返回匹配的实体列表 (List<T>)。通常是调用 repository.findBy...In(...) 方法。
- entityKeyGetter: 一个函数，用于从fetcher返回的实体中提取其自身的键，以便框架可以构建 Key -> Entity 的映射。

#### 使用示例：查找或创建客户

假设我们要导入订单，并且需要处理客户信息。

**第1步：在工作流开始时声明 .lookup()**

```java
WorkflowEngine.create(excelData, OrderContext::new)
    // 声明一个名为 "customers" 的查找缓存
    .lookup("customers",
        // 1. 从所有Excel行中提取出所有唯一的客户名称
        items -> items.stream()
                      .map(ExcelRow::customerName)
                      .collect(Collectors.toSet()),
        
        // 2. 使用这些名称，一次性从数据库查询已存在的客户
        customerNames -> customerRepo.findByNameIn(customerNames),
        
        // 3. 告诉框架如何从Customer实体中获取其名称（键）
        Customer::getName
    )
    // ... 后续的 .build, .relate, .persist 步骤 ...
```

**第2步：在 .build() 步骤中使用查找结果**

现在，在构建 Customer 实体的 .build() 步骤中，我们可以利用这个预加载的缓存。

```java
// ... 接上文 ...
.build(Customer.class,
    ExcelRow::customerName,
    (row, ctx) -> {
        // 从上下文中获取名为 "customers" 的查找缓存
        Map<String, Customer> existingCustomers = ctx.getLookup("customers");
        
        // 检查当前行的客户是否已经存在
        if (existingCustomers.containsKey(row.customerName())) {
            // 如果存在，我们不需要创建新的，返回null即可
            // 框架会自动跳过对已存在键的重复构建
            return null; 
        } else {
            // 如果不存在，则创建一个新的Customer实例等待持久化
            return new Customer(row.customerName());
        }
    })
// ...
```

通过这种方式，我们仅用了一次数据库查询就完成了所有客户的“查找或创建”逻辑，完美地避免了N+1问题，同时保持了代码的清晰和高效。
