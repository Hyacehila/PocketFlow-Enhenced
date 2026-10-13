---
layout: default
title: "异步并行重试状态隔离修复"
nav_exclude: true
---

# 异步并行重试状态隔离修复

本修复用于个人后续开发维护的 PocketFlow Python SDK。我们参考了 Iams4kura 提交的 [The-Pocket/PocketFlow PR #148：fix: isolate retry state in parallel batches](https://github.com/The-Pocket/PocketFlow/pull/148)，修复异步并行任务共用重试计数导致提前进入 fallback、遗漏 fallback 和读取错误序号的问题。

记录日期：2026-10-13。该参考 PR 在本次核对时仍未合并；本文描述的是本仓库的实现，并在其 ContextVar 隔离思路上加入局部控制计数和嵌套调用的状态恢复。修复基于官方 `f74d023` 的文件内容；此前个人分支的内置可视化功能及其他自定义内容已在独立的恢复步骤中撤销，个人提交历史继续保留。

## Bug 如何发生

原来的 `AsyncNode._exec()` 同时通过实例属性记录序号、决定是否耗尽尝试次数：

```python
for self.cur_retry in range(self.max_retries):
    ...
    if self.cur_retry == self.max_retries - 1:
        ...
```

`AsyncParallelBatchNode` 对每个 item 启动一个 `_exec(item)` 协程，但所有协程绑定同一个节点对象。一个任务在 `await` 处暂停时，其他任务可以改写这个对象的 `cur_retry`。

假设 `max_retries=3`，A 第一次失败、第二次能成功，B 前两次失败、第三次成功：

| 顺序 | 执行情况 | 共用的 `self.cur_retry` |
| --- | --- | --- |
| 1 | A 开始第一次执行，等待请求返回 | 0 |
| 2 | B 第一次执行失败，开始第二次 | 1 |
| 3 | B 第二次失败，第三次成功 | 2 |
| 4 | A 第一次执行返回异常 | 2 |
| 5 | A 误判尝试次数已耗尽，直接进入 fallback | 2 |

A 的循环迭代器仍处于第一次迭代，但异常处理读到的是 B 留下的对象属性。协程恢复时不会恢复节点对象的旧属性值；单线程 asyncio 也会发生这种交错。

相反的时序也会出错：A 正在最后一次尝试，B 把计数写回 `0`。A 失败后跳过 fallback，随后自己的循环耗尽，方法隐式返回 `None`。此外，`exec_async()` 或 `exec_fallback_async()` 在 `await` 后读取序号，也可能读到其他任务的值。

## 影响范围

- 同一个 `AsyncParallelBatchNode` 中重试进度不同的 item。
- 放在 `AsyncFlow` 中执行的并行批处理节点。Flow 的节点副本仍被内部所有 item 共用。
- 同一个普通 `AsyncNode` 实例的并发调用。
- 同一 Task 内对同一节点的嵌套调用，也需要保存并恢复外层重试状态。

顺序执行的单个异步节点通常无法暴露这个缺陷。`max_retries=3` 表示最多执行三次，包含第一次执行，序号为 `0、1、2`；成功时提前结束。

## 本仓库的修复

实现位于 [pocketflow/__init__.py](../../pocketflow/__init__.py)。

1. **局部变量控制重试。** `_exec()` 使用局部 `retry_index` 判断是否到最后一次，控制流程不再依赖业务代码可访问的 `self.cur_retry`。
2. **ContextVar 提供序号。** `AsyncNode.cur_retry` 保留可读写的整数接口，读取当前上下文中属于该节点的序号。
3. **每次调用建立独立状态。** 模块级 ContextVar 保存不可变的 `(节点, 序号, 外层状态)`。通过节点身份区分状态，浅复制的节点不会因共用一个实例 ContextVar 而混淆序号。
4. **退出时恢复外层状态。** `_exec()` 用 `try/finally` 重置 token；成功、失败和取消都恢复调用者上下文，同一节点重入时也能恢复外层序号。

参考 PR 使用实例级 ContextVar 和属性实现 Task 间隔离。本实现将状态放在模块级上下文中，因此无需改变异步节点或 Flow 的构造函数，也不会把 ContextVar 对象放入节点实例的复制数据中。

同步 `Node` 的重试实现保持原有行为；本修复使用标准库，不增加第三方依赖。并行批处理继续共用原节点实例，业务字段的累计方式保持原有行为。

## 使用与兼容性

框架为 `exec_async()`、`exec_fallback_async()` 设置当前调用的零起始 `self.cur_retry` 序号；在这次调用的 `await` 前后，其他任务不会改写这个上下文中的值。

需要注意一个可见行为变化：**异步执行结束后，`cur_retry` 恢复为调用者上下文中的值，不再保留“最后完成的任务”的序号。** 没有外层状态或显式赋值时返回 `0`，新建异步节点读取该属性也返回 `0`。

因此，`post_async()` 和 `run_async()` 返回后的代码不能用它统计整批 item 的尝试次数。需要这类统计时，应显式记录每个 item 的尝试次数。同步节点执行后读取 `cur_retry` 的行为不受本修复影响。

属性仍可赋值，但赋值只改变当前上下文中的可见序号，不改变框架的最大尝试次数。嵌套调用退出后会恢复进入该调用前的上下文。

本修复只隔离重试状态；业务自定义的其他实例字段仍由并发任务共用。使用该个人 SDK 的修复代码，应从本仓库安装，例如在仓库根目录执行 `python -m pip install -e .`。

## 回归测试

[tests/test_async_retry_isolation.py](../../tests/test_async_retry_isolation.py) 新增 10 项测试，覆盖多个子场景：

- 并行 item 的独立尝试次数、序号读取与结果顺序；同时覆盖直接执行、AsyncFlow 包装和重试等待。
- 最后一次失败必须进入 fallback，包括自定义返回值和默认抛异常。
- fallback 在 `await` 后仍能读取自己的序号。
- 普通 AsyncNode 实例的并发调用。
- 同一节点及其浅复制节点的嵌套调用，在内层成功或失败后恢复外层序号。
- 成功、业务异常、fallback 返回或抛异常后的上下文恢复。
- 执行中、重试等待中和 fallback 中取消时恢复上下文，并继续传播取消。
- `post_async()` 读取恢复后的调用者序号。
- 公共序号的赋值不改变重试预算。

并发测试用 asyncio.Event 固定交错顺序；业务是否失败由独立的实际调用次数决定。取消等待的测试用可控的异步等待替代重试 sleep，确保取消发生在指定阶段。四个解释器上，官方实现运行新增的 10 项测试均出现 17 个失败子场景，能够复现提前 fallback、返回 `None`、序号串扰及未恢复调用者状态；修复后这些测试全部通过。子场景数包含同一测试方法的不同参数组合，不等于测试方法数。

在仓库根目录运行：

```shell
python -B -m unittest discover -s tests -p test_async_retry_isolation.py -v
python -B -m unittest discover -s tests -q
```

本次验证结果：

| Python | 官方基线原有测试 | 新增重试测试 | 修复后完整测试集 |
| --- | --- | --- | --- |
| 3.11.14 | 56 项运行，54 项通过，2 项报错 | 10 项通过 | 66 项运行，64 项通过，同样 2 项报错 |
| 3.12.12 | 56 项运行，54 项通过，2 项报错 | 10 项通过 | 66 项运行，64 项通过，同样 2 项报错 |
| 3.13.9 | 56 项全部通过 | 10 项通过 | 66 项全部通过 |
| 3.14.0 | 56 项全部通过 | 10 项通过 | 66 项全部通过 |

3.11、3.12 的两项错误来自既有 `test_async_parallel_batch_flow.py` 中的 `test_multiple_batch_sizes` 和 `test_parallel_batch_flow`：聚合节点读取其他并行分支尚未写入的结果时抛出 `KeyError`。本次先在完全恢复为官方内容的工作区运行了原有测试，确认同样两项错误已存在，再运行修复后的完整测试集。这部分实现和测试保持官方原样；完整测试集在这两个 Python 版本上仍未全通过，相关问题需要另行处理。

本次完整测试集为官方的 56 项 SDK 测试与新增的 10 项重试测试，不包含此前个人分支的可视化测试。

## 来源

- [PR #148：并行批处理重试状态隔离](https://github.com/The-Pocket/PocketFlow/pull/148)：本次修复参考的方案。
- [PR #101：让异步节点可访问 self.cur_retry](https://github.com/The-Pocket/PocketFlow/pull/101)：将异步局部计数改成实例属性的历史修改。
- [PocketFlow 重试参数说明](https://the-pocket.github.io/PocketFlow/core_abstraction/node.html)：最大执行次数和零起始序号的定义。
