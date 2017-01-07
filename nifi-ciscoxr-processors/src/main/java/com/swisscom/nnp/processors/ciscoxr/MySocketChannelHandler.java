package com.swisscom.nnp.processors.ciscoxr;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.listen.dispatcher.AsyncChannelDispatcher;
import org.apache.nifi.processor.util.listen.event.Event;
import org.apache.nifi.processor.util.listen.event.EventFactory;
import org.apache.nifi.processor.util.listen.event.EventFactoryUtil;
import org.apache.nifi.processor.util.listen.handler.socket.StandardSocketChannelHandler;
import org.apache.nifi.processor.util.listen.response.socket.SocketChannelResponder;
import org.apache.nifi.stream.io.ByteArrayOutputStream;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * Created by alex on 07/01/17.
 */
public class MySocketChannelHandler<E extends Event<SocketChannel>> extends StandardSocketChannelHandler<E> {
    public MySocketChannelHandler(final SelectionKey key,
                                  final AsyncChannelDispatcher dispatcher,
                                  final Charset charset,
                                  final EventFactory<E> eventFactory,
                                  final BlockingQueue<E> events,
                                  final ComponentLog logger) {
        super(key, dispatcher, charset, eventFactory, events, logger);
    }


    private final ByteArrayOutputStream currBytes = new ByteArrayOutputStream(14096);
    @Override
        protected void processBuffer(final SocketChannel socketChannel, final ByteBuffer socketBuffer) throws InterruptedException, IOException {
            // get total bytes in buffer
            final int total = socketBuffer.remaining(); // Returns the number of elements between the current position and the limit.
            final InetAddress sender = socketChannel.socket().getInetAddress();
            while (socketBuffer.remaining() >= 12) { // First 12 Bytes are Headers
                int type = socketBuffer.getInt();
                int flags = socketBuffer.getInt();
                int length = socketBuffer.getInt();
                if (length > socketBuffer.remaining()) return; // the message is not yet complete
                // TODO: Address problem of Garbage Length Value, don't accept minus vals (though valid as spec says
                // nothing about Integer) and not wait for a 4GB message
                // Possible Strategies: If JSON, advance until next "0x00" message
                // If GPB, till some "magic byte"
                // Or drop the whole socketBuffer by marking it's last position as new start.
                final byte[] msgBytes = new byte[length]; // length is known
                socketBuffer.get(msgBytes);  // copy message into buffer and advance position in socketbuffer

                final SocketChannelResponder response = new SocketChannelResponder(socketChannel);
                final Map<String, String> metadata = EventFactoryUtil.createMapWithSender(sender.toString());
                metadata.put("cisco.xr.type",Integer.toString(type));
                metadata.put("cisco.xr.flags",Integer.toString(flags));
                final E event = eventFactory.create(msgBytes, metadata, response);
                events.offer(event);

                // Mark this as the start of the next message
                socketBuffer.mark();

            }
    }
}
