---
displayed_sidebar: docs
---

# materialized_views

`materialized_views` 提供有关所有物化视图的信息。

`materialized_views` 提供以下字段：

| **字段**                             | **描述**                                         |
| ------------------------------------ | ------------------------------------------------ |
| MATERIALIZED_VIEW_ID                 | 物化视图 ID。                                    |
| TABLE_SCHEMA                         | 物化视图所在的数据库名称。                       |
| TABLE_NAME                           | 物化视图名称。                                   |
| REFRESH_TYPE                         | 物化视图（刷新）类型，包括 `ROLLUP`（同步物化视图）、`ASYNC`（异步刷新物化视图）以及 `MANUAL`（手动刷新物化视图）。当此值为 `ROLLUP` 时，以下生效状态和刷新相关的字段为空。 |
| IS_ACTIVE                            | 是否生效，失效的物化视图不会被刷新和查询改写。   |
| INACTIVE_REASON                      | 失效的原因。                                     |
| PARTITION_TYPE                       | 物化视图分区类型。                               |
| TASK_ID                              | 物化视图刷新任务的 ID。                          |
| TASK_NAME                            | 物化视图刷新任务的名称。                         |
| LAST_REFRESH_START_TIME              | 最近一次刷新任务的开始时间。                     |
| LAST_REFRESH_FINISHED_TIME           | 最近一次刷新任务的结束时间。                     |
| LAST_REFRESH_DURATION                | 最近一次刷新任务的持续时间。                     |
| LAST_REFRESH_STATE                   | 最近一次刷新任务的状态。                         |
| LAST_REFRESH_FORCE_REFRESH           | 最近一次刷新任务是否强制刷新。                   |
| LAST_REFRESH_START_PARTITION         | 最近一次刷新任务的开始分区。                     |
| LAST_REFRESH_END_PARTITION           | 最近一次刷新任务的结束分区。                     |
| LAST_REFRESH_BASE_REFRESH_PARTITIONS | 最近一次刷新任务的基表分区。                     |
| LAST_REFRESH_MV_REFRESH_PARTITIONS   | 最近一次刷新任务刷新的分区。                   |
| LAST_REFRESH_ERROR_CODE              | 最近一次刷新任务的错误码。                       |
| LAST_REFRESH_ERROR_MESSAGE           | 最近一次刷新任务的错误信息。                     |
| TABLE_ROWS                           | 物化视图的数据行数，后台统计的近似值。           |
| MATERIALIZED_VIEW_DEFINITION         | 物化视图的 SQL 定义。                            |
| EXTRA_MESSAGE                        | 物化视图的额外信息。                             |
| QUERY_REWRITE_STATUS                 | 物化视图的查询改写状态。                         |
| CREATOR                              | 物化视图的创建者。                               |
| LAST_REFRESH_PROCESS_TIME            | 最近一次刷新任务的处理时间。                     |
| LAST_REFRESH_JOB_ID                  | 最近一次刷新任务的作业 ID。                      |
