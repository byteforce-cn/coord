package cn.byteforce.coord.sdk.internal.watch;

/**
 * A blockingly-consumable gRPC server-streaming call that can actually be **cancelled**.
 *
 * <p><b>为什么需要它</b>（第四轮 §3.14.3）：SDK 原来直接用 gRPC 的阻塞式 stub iterator
 * （{@code ConfigGrpc.newBlockingStub(ch).watch(req)}）并把它的 {@code Iterator} 交给
 * 订阅循环。阻塞式 iterator 有两个致命性质：
 *
 * <ol>
 *   <li>它的 {@code hasNext()} 会**阻塞在网络上**。因此 {@code cancel()} 只能置一个
 *       标志位，循环要等到**下一条事件到达**才会看到它——对一个不再产生事件的订阅，
 *       取消等于没做：任务、通道、回调永远挂着。</li>
 *   <li>流结束（正常关闭或任何错误）只表现为 {@code hasNext()} 返回 {@code false}
 *       或抛异常；没有取消句柄，也没有可区分的失败原因。</li>
 * </ol>
 *
 * <p>本接口把这两件事显式化：{@link #close()} 真的取消底层 {@code ClientCall} 并唤醒
 * 阻塞中的消费者；{@link #failure()} 给出流结束的原因（{@code null} = 正常结束），
 * 使上层能够区分"我们主动取消"、"服务端正常关闭"与"连接断了/被拒"。
 */
public interface CancellableStream extends AutoCloseable {

    /**
     * 是否还有下一条事件（**阻塞**，直到有事件、流结束或被取消）。
     *
     * @return 有事件返回 {@code true}；流结束/被取消返回 {@code false}
     */
    boolean hasNext();

    /**
     * 取下一条事件。仅在 {@link #hasNext()} 返回 {@code true} 后调用。
     */
    Object next();

    /**
     * 流结束（或出错）的原因；{@code null} 表示正常结束或尚未结束。
     *
     * <p>被 {@link #close()} 主动取消时返回 {@code null}（那不是故障）。
     */
    Throwable failure();

    /**
     * 取消底层 RPC 并唤醒阻塞中的消费者。幂等。
     */
    @Override
    void close();
}
