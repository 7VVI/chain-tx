package com.konors.chaintxcore.log;

import org.slf4j.LoggerFactory;

/**
 * @author zhangyh
 * @Date 2025/7/1 14:50
 * @desc
 */
public class ChainTxLoggerFactory {

    public static final String  SOFA_ARK_LOGGER_SPACE        = "com.konors.chaintxcore";

    private static final String SOFA_ARK_DEFAULT_LOGGER_NAME = "com.konors.chaintxcore";

    public static ChainTxLogger defaultLogger = getLogger(SOFA_ARK_DEFAULT_LOGGER_NAME);

    public static ChainTxLogger getLogger(Class<?> clazz) {
        if (clazz == null) {
            return null;
        }
        return getLogger(clazz.getCanonicalName());
    }

    public static ChainTxLogger getLogger(String name) {
        if (name == null || name.isEmpty()) {
            return null;
        }
        return new ChainTxLogger(LoggerFactory.getLogger(name));
    }

    public static ChainTxLogger getDefaultLogger() {
        return defaultLogger;
    }

}
