package fr.redfroggy.keycloak;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jboss.logging.Logger;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.MessageAttributeValue;
import software.amazon.awssdk.services.sns.model.PublishRequest;

import java.util.Map;


class SnsEventPublisher {

    private static final Logger log = Logger.getLogger(SnsEventPublisher.class);
    private final SnsClient snsClient;
    private final ObjectMapper mapper;
    private final SnsEventListenerConfiguration snsEventListenerConfiguration;

    public SnsEventPublisher(SnsClient snsClient, SnsEventListenerConfiguration snsEventListenerConfiguration,
                             ObjectMapper mapper) {
        this.snsClient = snsClient;
        this.snsEventListenerConfiguration = snsEventListenerConfiguration;
        this.mapper = mapper;
    }

    public void sendEvent(SnsEvent snsEvent, Map<String, MessageAttributeValue> messageAttributes) {
        if (snsEventListenerConfiguration.getEventTopicArn() == null) {
            log.warn(
                    "No topicArn specified. Can not send event to AWS SNS! Set environment variable KC_SNS_EVENT_TOPIC_ARN");
            return;
        }
        publishEvent(snsEvent, messageAttributes, snsEventListenerConfiguration.getEventTopicArn());
    }

    public void sendAdminEvent(SnsAdminEvent snsAdminEvent, Map<String, MessageAttributeValue> messageAttributes) {
        if (snsEventListenerConfiguration.getAdminEventTopicArn() == null) {
            log.warn(
                    "No topicArn specified. Can not send event to AWS SNS! Set environment variable KC_SNS_ADMIN_EVENT_TOPIC_ARN");
            return;
        }
        publishEvent(snsAdminEvent, messageAttributes, snsEventListenerConfiguration.getAdminEventTopicArn());
    }

    private void publishEvent(Object event, Map<String, MessageAttributeValue> messageAttributes, String topicArn) {
        try {
            var publishRequest = PublishRequest.builder()
                    .topicArn(topicArn)
                    .messageAttributes(messageAttributes)
                    .message(mapper.writeValueAsString(event))
                    .build();
            snsClient.publish(publishRequest);
        } catch (JsonProcessingException e) {
            log.error("The payload wasn't created.", e);
        } catch (Exception e) {
            log.error("Exception occured during the event publication", e);
        }
    }
}
