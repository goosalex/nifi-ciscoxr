/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.swisscom.nnp.processors.ciscoxr;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.listen.AbstractListenEventBatchingProcessor;
import org.apache.nifi.processor.util.listen.dispatcher.AsyncChannelDispatcher;
import org.apache.nifi.processor.util.listen.dispatcher.ChannelDispatcher;
import org.apache.nifi.processor.util.listen.dispatcher.SocketChannelDispatcher;
import org.apache.nifi.processor.util.listen.event.EventFactory;
import org.apache.nifi.processor.util.listen.event.StandardEvent;
import org.apache.nifi.processor.util.listen.event.StandardEventFactory;
import org.apache.nifi.processor.util.listen.handler.ChannelHandlerFactory;
import org.apache.nifi.processor.util.listen.handler.socket.SocketChannelHandlerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.BlockingQueue;

@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"listen", "tcp", "cisco-xr", "custom"})
@CapabilityDescription("Listens for incoming TCP connections and reads Cisco XR Telemtry data from each connection  " +
        "as defined in " +
        "http://www.cisco.com/c/en/us/td/docs/iosxr/Telemetry/Telemetry-Config-Guide/Telemetry-Config-Guide_chapter_0100.pdf ." +
        "<BR/>The default behavior is for each message to produce a single FlowFile, however this can " +
        "be controlled by increasing the Batch Size to a larger value for higher throughput. The Receive Buffer Size must be " +
        "set as large as the largest messages expected to be received, meaning if every 100kb there is a line separator, then " +
        "the Receive Buffer Size must be greater than 100kb.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="Reads ")})
@WritesAttributes({
        @WritesAttribute(attribute = "tcp.sender", description = "The sending host of the messages."),
        @WritesAttribute(attribute = "tcp.port", description = "The sending port the messages were received.")
})
public class MyProcessor extends AbstractListenEventBatchingProcessor<StandardEvent> {

    @Override
    protected List<PropertyDescriptor> getAdditionalProperties() {
        return Arrays.asList(
                MAX_CONNECTIONS
        );
    }

    @Override
    protected Map<String, String> getAttributes(FlowFileEventBatch batch) {
        final String sender = batch.getEvents().get(0).getSender();
        final Map<String,String> attributes = new HashMap<>(3);
        attributes.put("tcp.sender", sender);
        attributes.put("tcp.port", String.valueOf(port));
        return attributes;
    }

    @Override
    protected String getTransitUri(FlowFileEventBatch batch) {
        final String sender = batch.getEvents().get(0).getSender();
        final String senderHost = sender.startsWith("/") && sender.length() > 1 ? sender.substring(1) : sender;
        final String transitUri = new StringBuilder().append("ciscoxr").append("://").append(senderHost).append(":")
                .append(port).toString();
        return transitUri;
    }

    @Override
    protected ChannelDispatcher createDispatcher(ProcessContext context, BlockingQueue<StandardEvent> events) throws IOException {

        final int maxConnections = context.getProperty(MAX_CONNECTIONS).asInteger();
        final int bufferSize = context.getProperty(RECV_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        final Charset charSet = Charset.forName(context.getProperty(CHARSET).getValue());

        // initialize the buffer pool based on max number of connections and the buffer size
        final BlockingQueue<ByteBuffer> bufferPool = createBufferPool(maxConnections, bufferSize);

        // Falls man auch den message Type als Attribut ausgeben will, muss hier der StandardEvent um ein attibut erweiter werden und
        // Eine entsprechende Factory gemacht werden. Nicht schwer.
        final EventFactory<StandardEvent> eventFactory = new StandardEventFactory();
        final ChannelHandlerFactory<StandardEvent<SocketChannel>, AsyncChannelDispatcher> handlerFactory = new MyChannelHandlerFactory<>();
        return new SocketChannelDispatcher(eventFactory, handlerFactory, bufferPool, events, getLogger(), maxConnections, null, null, charSet);


    }
}
