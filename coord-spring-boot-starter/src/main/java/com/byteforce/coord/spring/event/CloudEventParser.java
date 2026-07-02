package cn.byteforce.coord.spring.event;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * CloudEvents 1.0 解析器
 *
 * 构建符合 CloudEvents 1.0 规范的事件对象。
 */
public final class CloudEventParser {

    private CloudEventParser() {}

    /**
     * 构建 CloudEvent
     *
     * @param type   事件类型
     * @param source 事件来源
     * @param data   事件数据（JSON 字节）
     * @return CloudEvent 对象
     */
    public static CloudEvent build(String type, String source, byte[] data) {
        return CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withType(type)
                .withSource(URI.create(source))
                .withData("application/json", data)
                .withTime(OffsetDateTime.now())
                .build();
    }

    /**
     * 构建 CloudEvent（文本数据）
     */
    public static CloudEvent buildText(String type, String source, String text) {
        return CloudEventBuilder.v1()
                .withId(UUID.randomUUID().toString())
                .withType(type)
                .withSource(URI.create(source))
                .withData("text/plain", text.getBytes(java.nio.charset.StandardCharsets.UTF_8))
                .withTime(OffsetDateTime.now())
                .build();
    }

    /**
     * 验证 CloudEvent 是否至少包含必填字段 (specversion, type, source, id)
     */
    public static boolean isValid(CloudEvent event) {
        return event.getSpecVersion() != null
                && event.getType() != null && !event.getType().isEmpty()
                && event.getSource() != null
                && event.getId() != null && !event.getId().isEmpty();
    }
}
