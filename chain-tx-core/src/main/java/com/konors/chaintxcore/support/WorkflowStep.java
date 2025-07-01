package com.konors.chaintxcore.support;

/**
 * @author zhangyh
 * @Date 2025/7/1 14:33
 * @desc 代表工作流中的一个可执行步骤。
 * @param <C> 上下文类型
 */
@FunctionalInterface
public interface WorkflowStep<C> {
    void execute(C context) throws Exception;
}

