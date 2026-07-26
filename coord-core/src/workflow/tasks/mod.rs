// coord-core/workflow/tasks/mod.rs
// 工作流任务执行模块 —— 每种任务类型独立子模块
//
// 每个任务模块导出一个纯函数，接收任务定义和上下文，
// 返回 StepResult。执行器不执行 I/O，所有副作用由 Runtime 处理。
//
// 已实现:
// - call:    外部调用（HTTP/gRPC/function）→ Suspend
// - do:      顺序子任务 → NextTask
// - switch:  条件分支 → Goto
// - wait:    定时等待 → Suspend
// - set:     变量赋值 → SetVariable
// - raise:   错误抛出 → Failed
// - emit:    事件发布 → NextTask（Runtime 负责 EventProvider.emit）
// - listen:  事件监听 → Suspend(ListeningForEvent)
// - fork:    并行分支 → Fork（Runtime 负责分支调度）
// - for_each:集合迭代 → ForEach（Runtime 负责迭代执行）
// - try_catch:异常处理 → TryBlock（Runtime 负责 try/catch 流程）
// - run:     子流程 → Suspend(RunSubflow)

pub mod call;
pub mod do_task;
pub mod switch;
pub mod wait;
pub mod set;
pub mod raise;
pub mod emit;
pub mod listen;
pub mod fork;
pub mod for_each;
pub mod try_catch;
pub mod run;
