package org.urbcomp.startdb.stkq.stream;

public enum QueryDistributionEnum {
    UNIFORM,    // 均匀分布，任何时刻的查询个数相同
    GEOMETRIC,  // 几何分布，当前时刻的查询个数最多，上一时刻是当前时刻的一半，依次类推
    LINEAR      // 线性分布，当前时刻的查询个数最多，不同时刻的查询个数呈线性关系
}
