//! 输出一个「会签 + 加签 + 一票否决前」的场景 JSON，作为报告原始数据。
//! 运行：`cargo run --example dump_scenario`

use spike_approval_model::*;

fn main() {
    let mut e = Engine::start(
        "eis",
        "leave-approval",
        "1.0.0",
        "zhangsan",
        Scope::new(),
        1_000,
    );
    let mut t = 1_000;
    let mut next = || {
        t += 100;
        t
    };
    let (b, _) = e
        .enter_branch(
            e.instance.revision,
            "apply",
            None,
            JoinPolicy::All,
            RejectPolicy::AnyReject,
            next(),
        )
        .unwrap();
    let (_, seats, _) = e
        .open_approval(
            e.instance.revision,
            &b,
            vec![
                SeatSpec::approver("alice"),
                SeatSpec::approver("bob"),
                SeatSpec::approver("carol"),
            ],
            None,
            next(),
        )
        .unwrap();
    e.approve(
        e.instance.revision,
        &seats[0],
        "alice",
        Some("同意"),
        next(),
    )
    .unwrap();
    e.add_sign(
        e.instance.revision,
        &b,
        vec![SeatSpec::approver("dave")],
        "admin",
        Some("追加分管领导"),
        next(),
    )
    .unwrap();
    e.delegate(e.instance.revision, &seats[1], "dave", "bob", next())
        .unwrap();
    e.approve(e.instance.revision, &seats[1], "dave", Some("代批"), next())
        .unwrap();
    e.approve(
        e.instance.revision,
        &seats[2],
        "carol",
        Some("同意"),
        next(),
    )
    .unwrap();
    let dave_task = e
        .tasks
        .values()
        .find(|x| x.assignee.as_deref() == Some("dave") && x.status.is_open())
        .unwrap()
        .id
        .clone();
    e.reject(
        e.instance.revision,
        &dave_task,
        "dave",
        Some("不同意"),
        next(),
    )
    .unwrap();

    println!(
        "{}",
        serde_json::to_string_pretty(&e.snapshot_json()).unwrap()
    );
}
