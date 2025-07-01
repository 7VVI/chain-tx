package com.konors.chaintxcore;

import com.konors.chaintxcore.support.WorkflowContext;
import com.konors.chaintxcore.support.WorkflowEngine;
import lombok.Data;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

@SpringBootTest
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

}
