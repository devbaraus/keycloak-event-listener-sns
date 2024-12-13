package fr.redfroggy.keycloak;

import org.jboss.logging.Logger;
import org.keycloak.events.Event;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.models.RealmProvider;
import org.keycloak.models.UserModel;
import org.keycloak.models.UserProvider;
import software.amazon.awssdk.services.sns.model.MessageAttributeValue;

import java.util.HashMap;
import java.util.Map;


public class SnsEventListenerProvider implements EventListenerProvider {
    private final Logger log = Logger.getLogger(SnsEventListenerProvider.class.getName());

    private final SnsEventPublisher snsEventPublisher;
    private final UserProvider userProvider;
    private final RealmProvider realmProvider;

    public SnsEventListenerProvider(SnsEventPublisher snsEventPublisher,
                                    UserProvider userProvider, RealmProvider realmProvider) {
        this.snsEventPublisher = snsEventPublisher;
        this.userProvider = userProvider;
        this.realmProvider = realmProvider;
    }

    @Override
    public void onEvent(Event event) {
        Map<String, MessageAttributeValue> messageAttibutes = new HashMap<String, MessageAttributeValue>();

        String eventType = (event.getRealmName().toUpperCase() + ":" + event.getType()).toUpperCase();
        MessageAttributeValue messageEventType = MessageAttributeValue.builder().dataType("String").stringValue(eventType).build();
        messageAttibutes.put("eventType", messageEventType);

        log.info("[SNS] Sending event: " + eventType);
        snsEventPublisher.sendEvent(new SnsEvent(event, getUsername(event.getRealmId(), event.getUserId())), messageAttibutes);
    }

    @Override
    public void onEvent(AdminEvent event, boolean includeRepresentation) {
        Map<String, MessageAttributeValue> messageAttibutes = new HashMap<String, MessageAttributeValue>();
        String adminUserId = null;

        if (event.getAuthDetails() != null) {
            adminUserId = event.getAuthDetails().getUserId();
        }

        String eventType = (event.getRealmName() + ":" + event.getResourceType() + "-" + event.getOperationType()).toUpperCase();
        MessageAttributeValue messageEventType = MessageAttributeValue.builder().dataType("String").stringValue(eventType).build();
        messageAttibutes.put("eventType", messageEventType);

        log.info("[SNS] Sending admin event: " + eventType);
        snsEventPublisher.sendAdminEvent(new SnsAdminEvent(event, getUsername(event.getRealmId(), adminUserId)), messageAttibutes);
    }

    @Override
    public void close() {
    }


    private String getUsername(String realmId, String userId) {
        UserModel user;
        if (userId != null) {
            user = userProvider.getUserById(realmProvider.getRealm(realmId), userId);

            if (user != null) {
                return user.getUsername();
            }
        }
        return null;
    }
}
