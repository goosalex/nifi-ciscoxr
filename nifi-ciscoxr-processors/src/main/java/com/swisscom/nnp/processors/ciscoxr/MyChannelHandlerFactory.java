package com.swisscom.nnp.processors.ciscoxr;

/**
 * Created by alex on 07/01/17.
 */
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.listen.dispatcher.AsyncChannelDispatcher;
import org.apache.nifi.processor.util.listen.event.Event;
import org.apache.nifi.processor.util.listen.event.EventFactory;
import org.apache.nifi.processor.util.listen.handler.ChannelHandler;
import org.apache.nifi.processor.util.listen.handler.ChannelHandlerFactory;

import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.concurrent.BlockingQueue;

/**
 * Default factory for creating socket channel handlers.
 */
public class MyChannelHandlerFactory<E extends Event<SocketChannel>> implements ChannelHandlerFactory<E, AsyncChannelDispatcher> {

    @Override
    public ChannelHandler<E, AsyncChannelDispatcher> createHandler(final SelectionKey key,
                                                                   final AsyncChannelDispatcher dispatcher,
                                                                   final Charset charset,
                                                                   final EventFactory<E> eventFactory,
                                                                   final BlockingQueue<E> events,
                                                                   final ComponentLog logger) {
        return new MySocketChannelHandler<>(key, dispatcher, charset, eventFactory, events, logger);
    }

    @Override
    public ChannelHandler<E, AsyncChannelDispatcher> createSSLHandler(final SelectionKey key,
                                                                      final AsyncChannelDispatcher dispatcher,
                                                                      final Charset charset,
                                                                      final EventFactory<E> eventFactory,
                                                                      final BlockingQueue<E> events,
                                                                      final ComponentLog logger) {
        return createHandler(key, dispatcher, charset, eventFactory, events, logger);
    }
}
