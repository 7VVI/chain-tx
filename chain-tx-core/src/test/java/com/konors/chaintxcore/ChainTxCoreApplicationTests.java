package com.konors.chaintxcore;

import com.konors.chaintxcore.assembler.DataAssembler;
import com.konors.chaintxcore.support.WorkflowContext;
import com.konors.chaintxcore.support.WorkflowEngine;
import lombok.Data;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

//@SpringBootTest
class ChainTxCoreApplicationTests {

    // --- 模拟的源数据 DTO ---
    @Data
    static class StreamExcelVo {
        private String mountpoint;
        private String receiverCode;

        private String countryName;

        public StreamExcelVo(String mountpoint, String receiverCode, String countryName) {
            this.mountpoint = mountpoint;
            this.receiverCode = receiverCode;
            this.countryName = countryName;
        }
    }

    // --- 模拟的实体类 ---
    static class Country {
        private Long   id;
        private String name;

        public Country(String name) {
            this.name = name;
        }

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        @Override
        public String toString() {
            return "Country{id=" + id + ", name='" + name + "'}";
        }
    }

    static class Receiver {
        private Long   id;
        private String code;
        private Long   countryId;

        public Receiver(String code) {
            this.code = code;
        }

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getCode() {
            return code;
        }

        public void setCountryId(Long countryId) {
            this.countryId = countryId;
        }

        @Override
        public String toString() {
            return "Receiver{id=" + id + ", code='" + code + "', countryId=" + countryId + "}";
        }
    }

    static class Mountpoint {
        private Long   id;
        private String name;
        private Long   receiverId;

        public Mountpoint(String name) {
            this.name = name;
        }

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setReceiverId(Long receiverId) {
            this.receiverId = receiverId;
        }

        @Override
        public String toString() {
            return "Mountpoint{id=" + id + ", name='" + name + "', receiverId=" + receiverId + "}";
        }
    }

    // --- 模拟的特定业务上下文 ---
    static class ExcelContext extends WorkflowContext<StreamExcelVo> {
        public ExcelContext(List<StreamExcelVo> sourceItems) {
            super(sourceItems);
        }
    }

    // --- 模拟的数据库仓库(Repository) ---
    static class MockRepository<T> {
        private final AtomicLong idGenerator = new AtomicLong(0);

        public void saveAll(List<T> entities) {
            System.out.println("  -> Mock saving " + entities.size() + " " + entities.get(0).getClass().getSimpleName() + "s...");
            for (T entity : entities) {
                try {
                    Method setId = entity.getClass().getMethod("setId", Long.class);
                    setId.invoke(entity, idGenerator.incrementAndGet());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            System.out.println("  -> Saved entities: " + entities);
        }

        public List<T> findByKeys(Set<?> keys, Function<T, ?> keyGetter) {
            return Collections.emptyList(); /* 模拟DB中无数据 */
        }
    }

    @Test
    void contextLoads() {
        // --- 准备数据 ---
        List<StreamExcelVo> excelData = List.of(
                new StreamExcelVo("MP001", "R001", "China"),
                new StreamExcelVo("MP002", "R002", "USA"),
                new StreamExcelVo("MP003", "R001", "China") // R001被复用
        );

        // --- 模拟的仓库实例 ---
        MockRepository<Country> countryRepo = new MockRepository<>();
        MockRepository<Receiver> receiverRepo = new MockRepository<>();
        MockRepository<Mountpoint> mountpointRepo = new MockRepository<>();

        // --- 🚀 使用工作流引擎 🚀 ---
        WorkflowEngine.create(excelData, ExcelContext::new)

                // 1. 声明：预加载国家数据
                .lookup("countries",
                        items -> items.stream().map(StreamExcelVo::getCountryName).collect(Collectors.toSet()),
                        keys -> countryRepo.findByKeys(keys, Country::getName),
                        Country::getName)
                // 2. 声明：如何构建实体
                .build(Country.class,
                        StreamExcelVo::getCountryName,
                        (item, ctx) -> {
                            // "查找或创建"逻辑：如果lookup中没有，就创建一个新的
                            if (ctx.getLookup("countries").containsKey(item.getCountryName())) {
                                return null; // 已经存在，无需构建
                            }
                            return new Country(item.getCountryName());
                        })
                .build(Receiver.class,
                        StreamExcelVo::getReceiverCode,
                        (item, ctx) -> new Receiver(item.getReceiverCode()))
                .build(Mountpoint.class,
                        StreamExcelVo::getMountpoint,
                        (item, ctx) -> new Mountpoint(item.getMountpoint()))

                // 3. 声明：实体间的关联关系
                .relate(Receiver.class, Receiver::setCountryId, Country.class,
                        StreamExcelVo::getCountryName, "Receiver-Country")
                .relate(Mountpoint.class, Mountpoint::setReceiverId, Receiver.class,
                        StreamExcelVo::getReceiverCode, "Mountpoint-Receiver")

                // 4. 声明：如何持久化实体
                .persist(Country.class, countryRepo::saveAll, Country::getId)
                .persist(Receiver.class, receiverRepo::saveAll, Receiver::getId)
                .persist(Mountpoint.class, mountpointRepo::saveAll, Mountpoint::getId)

                // 5. 执行！
                .executeWithoutTransaction();

        System.out.println("\nWorkflow finished successfully!");
    }

    @Test
    void testFluent() {
//        AsyncDataFetcher fetcher = new AsyncDataFetcher(executor);

//        DataKey<Map<String, User>> authorMapKey = fetcher.register("authorMap", () -> userService.findUserMapByIds(authorIds));
//        DataKey<Map<String, List<LikeUserDto>>> likeMapKey = fetcher.register("likeMap", () -> likeService.findLikeUsersMapByVideoIds(videoIds));
//        DataKey<Map<String, Long>> attachmentCountKey = fetcher.register("attachmentCount", () -> attachmentService.countAttachmentsByVideoIds(videoIds));
//
//        // --- 阶段二：执行并组装 ---
//        return fetcher.executeAndAssemble(videos, (video, data) -> {
//            // 在这个Lambda中，所有数据都已准备好，可以通过 key 安全获取
//            Map<String, User> authorMap = data.get(authorMapKey);
//            Map<String, List<LikeUserDto>> likeMap = data.get(likeMapKey);
//            Map<String, Long> attachmentCountMap = data.get(attachmentCountKey);
//
//            // 使用 FluentAssembler 进行组装
//            return FluentAssembler.from(video, VideoVo::new)
//                    .copyProperties() // 复制 id, title
//                    .set(VideoVo::setThumbnailUrl, () -> CDN_BASE_URL + video.getThumbnailPath())
//                    .set(VideoVo::setAuthorName, () ->
//                            Optional.ofNullable(authorMap.get(video.getAuthorId()))
//                                    .map(User::getRealname)
//                                    .orElse("Unknown Author")
//                    )
//                    .set(VideoVo::setLikeUsers, () ->
//                            likeMap.getOrDefault(video.getId(), Collections.emptyList())
//                    )
//                    .set(VideoVo::setLikeCount, () ->
//                            likeMap.getOrDefault(video.getId(), Collections.emptyList()).size()
//                    )
//                    .set(VideoVo::setAttachmentCount, () ->
//                            attachmentCountMap.getOrDefault(video.getId(), 0L)
//                    )
//                    .get();
//        });
    }

    // 原始数据模型
    record User(int id, String name, String city) {}
    record Order(int orderId, int userId, int productId, double amount) {}
    record Product(int pid, String productName) {}

    // 中间组装结果模型
    record UserOrder(User user, Order order) {}

    // 最终组装结果模型
    record FullOrderInfo(int userId, String userName, String city, int orderId, double amount, String productName) {
        @Override
        public String toString() {
            return String.format(
                    "FullOrderInfo[user=%s(%d), city=%s, orderId=%d, product=%s, amount=%.2f]",
                    userName, userId, city, orderId, productName, amount
            );
        }
    }

    @Test
    void testAssembler() {
        // 1. 准备原始数据
        List<User> users = Arrays.asList(
                new User(1, "Alice", "New York"),
                new User(2, "Bob", "London"),
                new User(3, "Charlie", "Paris") // 该用户没有订单
        );

        List<Order> orders = Arrays.asList(
                new Order(101, 1, 10, 150.0),
                new Order(102, 2, 20, 200.0),
                new Order(103, 1, 20, 220.0)
        );

        List<Product> products = Arrays.asList(
                new Product(10, "Laptop"),
                new Product(20, "Mouse"),
                new Product(30, "Keyboard") // 该产品没有被购买
        );

        System.out.println("--- 泛型链式组装 (INNER JOIN) ---");

        // 2. 使用泛型DataAssembler进行链式组装
        List<FullOrderInfo> finalData = DataAssembler
                .source(users) // 返回 DataAssembler<User>
                .data(orders)  // 返回 PairedDataAssembler<User, Order>
                .match((user, order) -> user.id() == order.userId())
                .assemble((user, order) -> new UserOrder(user, order)) // 返回 DataAssembler<UserOrder>
                .data(products) // 返回 PairedDataAssembler<UserOrder, Product>
                .match((userOrder, product) -> userOrder.order().productId() == product.pid())
                .assemble((userOrder, product) -> new FullOrderInfo( // 返回 DataAssembler<FullOrderInfo>
                        userOrder.user().id(),
                        userOrder.user().name(),
                        userOrder.user().city(),
                        userOrder.order().orderId(),
                        userOrder.order().amount(),
                        product.productName()
                ))
                .get(); // 返回 List<FullOrderInfo>

        finalData.forEach(System.out::println);


        System.out.println("\n--- 泛型链式组装 (LEFT JOIN) ---");

        // 3. 使用 leftAssemble 保留所有用户
        List<FullOrderInfo> leftJoinData = DataAssembler
                .source(users) // -> DataAssembler<User>
                .data(orders)  // -> PairedDataAssembler<User, Order>
                .match((user, order) -> user.id() == order.userId())
                //  始终返回统一的中间类型 UserOrder。
                // 如果没有匹配的订单，则order字段为null。
                .leftAssemble((user, order) -> new UserOrder(user, order)) // -> DataAssembler<UserOrder>

                .data(products) // -> PairedDataAssembler<UserOrder, Product>
                // 匹配时，必须先检查中间对象中的order是否存在。
                .match((userOrder, product) ->
                        userOrder.order() != null && // 关键检查！
                                userOrder.order().productId() == product.pid()
                )
                //在最终组装时，处理所有可能的情况。
                .leftAssemble((userOrder, product) -> {
                    User user = userOrder.user();
                    Order order = userOrder.order();

                    // 情况 A: 用户有订单，且产品信息也找到了
                    if (order != null && product != null) {
                        return new FullOrderInfo(
                                user.id(), user.name(), user.city(),
                                order.orderId(), order.amount(),
                                product.productName()
                        );
                    }
                    // 情况 B: 用户有订单，但产品信息未找到 (product is null)
                    else if (order != null) {
                        return new FullOrderInfo(
                                user.id(), user.name(), user.city(),
                                order.orderId(), order.amount(),
                                "Product Not Found" // 或者 null, 或其他标记
                        );
                    }
                    // 情况 C: 用户没有订单 (order is null, product will also be null)
                    else {
                        return new FullOrderInfo(
                                user.id(), user.name(), user.city(),
                                0, 0.0, "N/A"
                        );
                    }
                }) // -> DataAssembler<FullOrderInfo>
                .get(); // -> List<FullOrderInfo>

        leftJoinData.forEach(System.out::println);

        System.out.println("--- 高性能组装 (matchOn) 演示 ---");

        List<FullOrderInfo> result = DataAssembler
                .source(users)
                .data(orders)
                // 使用高性能的 matchOn 方法
                .matchOn(User::id, Order::userId)
                .leftAssemble((user, order) -> new UserOrder(user, order))

                .data(products)
                .matchOn(
                        userOrder -> userOrder.order() != null ? userOrder.order().productId() : null,
                        Product::pid
                )
                .leftAssemble((userOrder, product) -> {
                    User user = userOrder.user();
                    Order order = userOrder.order();
                    if (order != null && product != null) {
                        return new FullOrderInfo(user.id(), user.name(), user.city(), order.orderId(), order.amount(), product.productName());
                    } else if (order != null) {
                        return new FullOrderInfo(user.id(), user.name(), user.city(), order.orderId(), order.amount(), "Product Not Found");
                    } else {
                        return new FullOrderInfo(user.id(), user.name(), user.city(), 0, 0.0, "N/A");
                    }
                })
                .sorted(Comparator.comparing(FullOrderInfo::userName))
                .get();

        result.forEach(System.out::println);
    }

}
