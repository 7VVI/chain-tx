package com.konors.chaintxcore.exception;

/**
 * @author zhangyh
 * @Date 2025/7/1 14:51
 * @desc
 */
public class ChainTxRuntimeException extends RuntimeException {

    public ChainTxRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public ChainTxRuntimeException(Throwable cause) {
        super(cause);
    }

    public ChainTxRuntimeException(String message) {
        super(message);
    }
}
