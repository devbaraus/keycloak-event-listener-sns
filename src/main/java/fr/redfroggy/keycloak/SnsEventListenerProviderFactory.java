package fr.redfroggy.keycloak;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.jboss.logging.Logger;
import org.keycloak.Config;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventListenerProviderFactory;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;
import software.amazon.awssdk.services.sns.SnsClient;



public class SnsEventListenerProviderFactory implements EventListenerProviderFactory {

    public static final String SNS_EVENT_LISTENER = "aws-sns";
    private SnsEventListenerConfiguration snsEventListenerConfiguration;
    private String CONFIG_EVENT_TOPIC_ARN = "event-topic-arn";
    private String CONFIG_ADMIN_EVENT_TOPIC_ARN = "admin-event-topic-arn";
    private static final Logger log = Logger.getLogger(SnsEventListenerProviderFactory.class);

    @Override
    public void close() {        
    }

    @Override
    public EventListenerProvider create(KeycloakSession session) {
        SnsClient snsClient = SnsClient.builder().build();
        ObjectMapper mapper = new ObjectMapper();
        return new SnsEventListenerProvider(new SnsEventPublisher(snsClient, snsEventListenerConfiguration, mapper), session.users(), session.realms());
    }

    @Override
    public String getId() {
        return SNS_EVENT_LISTENER;
    }

    @Override
    public void init(Config.Scope config) {
        String configEventTopicArn = config.get(CONFIG_EVENT_TOPIC_ARN, System.getenv("KC_SNS_EVENT_TOPIC_ARN"));
        String configAdminEventTopicArn = config.get(CONFIG_ADMIN_EVENT_TOPIC_ARN, System.getenv("KC_SNS_ADMIN_EVENT_TOPIC_ARN")); 
        log.info("Configuration to publish event in dedicated sns topic : " + configEventTopicArn);
        log.info("Configuration to publish admin event in dedicated sns topic : " + configAdminEventTopicArn);
        snsEventListenerConfiguration = new SnsEventListenerConfiguration(configEventTopicArn, configAdminEventTopicArn);
    }

    @Override
    public void postInit(KeycloakSessionFactory sessionFactory) {
    }

}
