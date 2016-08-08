package com.oasis.logging.sink.test;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.log4j.Logger;

/**
 * Created by BG246070 on 2016/8/5.
 */
public class ConsoleSink extends AbstractSink implements Configurable {

    Logger logger = Logger.getLogger(ConsoleSink.class);

    @Override
    public Status process() throws EventDeliveryException {
        Status result = Status.READY;

        Channel channel = getChannel();

        Transaction transaction;
        transaction = channel.getTransaction();
        transaction.begin();
        Event event = channel.take();

        if (event != null)
            logger.info(event.getHeaders() + ":" + event.getBody());
        else {
            logger.info("event is null");
        }


        try {
            Thread.sleep(10000);
            logger.info("sleep 10s to act as a very slow sink");

        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        transaction.commit();
        transaction.close();
        return result;

    }

    @Override
    public void configure(Context context) {

    }
}
