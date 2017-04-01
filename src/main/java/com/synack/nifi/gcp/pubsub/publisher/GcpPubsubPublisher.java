
package com.synack.nifi.gcp.pubsub.publisher;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.ByteArray;
import com.google.cloud.pubsub.Message;
import com.google.cloud.pubsub.PubSub;
import com.google.cloud.pubsub.PubSubOptions;
import com.google.cloud.pubsub.Topic;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

/**
 *
 * @author Mikhail Sosonkin
 */
@Tags({"gcp", "pubsub", "publish"})
@CapabilityDescription("Publish to a GCP Pubsub topic")
@SeeAlso({})
@ReadsAttributes({
    @ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({
    @WritesAttribute(attribute = "filename", description = "name of the flow based on time"),
    @WritesAttribute(attribute = "ack_id", description = "GCP meassge ACK id")})
public class GcpPubsubPublisher extends AbstractProcessor {

    public static final PropertyDescriptor authProperty = new PropertyDescriptor.Builder().name("Authentication Keys")
            .description("Required if outside of GCP. OAuth token (contents of myproject.json)")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor topicProperty = new PropertyDescriptor.Builder().name("Topic")
            .description("Name of topic")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor projectIdProperty = new PropertyDescriptor.Builder().name("Project ID")
            .description("Project ID")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor batchProperty = new PropertyDescriptor.Builder().name("Batch size")
            .description("Max number of messages to send at a time")
            .required(true)
            .defaultValue("100")
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .build();
    
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed to be published")
            .build();

    static final Relationship REL_TOOBIG = new Relationship.Builder()
            .name("toobig")
            .description("FlowFiles that are too big to be published")
            .build();
    
    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;
    private PubSub pubsub;
    private Topic topic;
    private static int MAX_FLOW_SIZE = 9000000;
    
    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(authProperty);
        descriptors.add(topicProperty);
        descriptors.add(projectIdProperty);
        descriptors.add(batchProperty);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_FAILURE);
        relationships.add(REL_TOOBIG);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        if(topic == null) {
            try {
                PubSubOptions.Builder opts = PubSubOptions.newBuilder().setProjectId(context.getProperty(projectIdProperty).getValue());
                
                PropertyValue authKeys = context.getProperty(authProperty);
                if(authKeys.isSet()) {
                    opts = opts.setCredentials(ServiceAccountCredentials.fromStream(new ByteArrayInputStream(authKeys.getValue().getBytes())));
                }
                
                pubsub = opts.build().getService();
                
            } catch (IOException ex) {
                throw new ProcessException("Unable to open a pubsub", ex);
            }

            if(pubsub == null) {
                throw new ProcessException("Pubsub not initialized");
            }

            String topicName = context.getProperty(topicProperty).getValue();
            topic = pubsub.getTopic(topicName);

            if(topic == null) {
                throw new ProcessException("Topic not initialized: " + topicName);
            }
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        boolean doCommit = false;
        
        if(pubsub == null || topic == null) {
            throw new ProcessException("Context not initialized");
        }
        
        // do the normal flow stuff.
        int batch = context.getProperty(batchProperty).asLong().intValue();
        int counts = session.getQueueSize().getObjectCount();
        counts = Math.min(batch, counts);
        
        // get as many messages as we can
        List<FlowFile> flowFiles = session.get(counts);
        if (flowFiles.size() == 0) {
            return;
        }
        
        long totalSize = 0;
        List<FlowFile> toProcess = new ArrayList<>(counts);
        for(FlowFile flowFile : flowFiles) {
            if(flowFile.getSize() > MAX_FLOW_SIZE) {
                doCommit = true;
                session.transfer(flowFile, REL_TOOBIG);
            } else {
                totalSize += flowFile.getSize();

                if(totalSize < MAX_FLOW_SIZE) {
                    toProcess.add(flowFile);
                } else {
                    // single publish request cannot exceed 10MB
                    break;
                }
            }
        }
        
        final List<Message> msgs = new ArrayList<>(counts);
        
        // obtain the contents
        for(FlowFile flowFile : toProcess) {
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(InputStream in) throws IOException {
                    msgs.add(Message.of(ByteArray.copyFrom(in)));
                }
            });
        }
        
        // upload the messages and clean up local flows.
        topic.publish(msgs);
        
        for(FlowFile flowFile : toProcess) {
            doCommit = true;
            session.remove(flowFile);
        }
        
        if(doCommit) {
            session.commit();
        }
    }
}
