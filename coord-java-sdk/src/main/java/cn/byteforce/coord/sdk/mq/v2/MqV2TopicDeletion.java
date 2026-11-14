package cn.byteforce.coord.sdk.mq.v2;

/**
 * Reclamation progress of one {@code DeleteTopic}. {@code partitionsReclaimed}
 * reaching {@code partitionsTotal} with state {@code DONE} is the terminal
 * condition; a same-named topic recreated in the meantime is a different
 * {@code topicId} and is not affected.
 */
public record MqV2TopicDeletion(String deleteId, String topic, long topicId,
                                MqV2DeletionState state, int partitionsTotal,
                                int partitionsReclaimed, long messagesRemoved,
                                long bytesReclaimed) {}
