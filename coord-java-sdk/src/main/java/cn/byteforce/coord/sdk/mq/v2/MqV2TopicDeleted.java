package cn.byteforce.coord.sdk.mq.v2;

/**
 * Result of {@code DeleteTopic}. The delete is immediate for readers/writers of
 * the old {@code topicId}; reclamation is asynchronous — poll
 * {@link MqV2Client#getTopicDeletion(String)} with {@code deleteId}.
 */
public record MqV2TopicDeleted(String deleteId, long deletedTopicEpoch) {}
