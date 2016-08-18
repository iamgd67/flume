/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * limitations under the License.
 */

package org.apache.flume.sink.kafka;

import com.google.common.base.Throwables;
import com.oasis.logging.kafkasink.MyMessagePartitionerOld;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.kafka.KafkaSinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;

/**
 * A Flume Sink that can publish messages to Kafka.
 * This is a general implementation that can be used with any Flume agent and
 * a channel.
 * The message can be any event and the key is a string that we read from the
 * header
 * For use of partitioning, use an interceptor to generate a header with the
 * partition key
 * <p/>
 * Mandatory properties are:
 * brokerList -- can be a partial list, but at least 2 are recommended for HA
 * <p/>
 * <p/>
 * however, any property starting with "kafka." will be passed along to the
 * Kafka producer
 * Read the Kafka producer documentation to see which configurations can be used
 * <p/>
 * Optional properties
 * topic - there's a default, and also - this can be in the event header if
 * you need to support events with
 * different topics
 * batchSize - how many messages to process in one batch. Larger batches
 * improve throughput while adding latency.
 * requiredAcks -- 0 (unsafe), 1 (accepted by at least one broker, default),
 * -1 (accepted by all brokers)
 * <p/>
 * header properties (per event):
 * topic
 * key
 */
public class KafkaSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(KafkaSink.class);
    public static final String KEY_HDR = "key";
    public static final String TOPIC_HDR = "topic";
    private Properties kafkaProps;
    private Producer<String, byte[]> producer;
    private String topic;
    private int batchSize;
    private List<KeyedMessage<String, byte[]>> messageList;
    private KafkaSinkCounter counter;


    //kafka meesage with header
    //采用和1.7版本兼容的方式

    ByteArrayOutputStream otstream = new ByteArrayOutputStream(1024);
    BinaryEncoder encoder;
    SpecificDatumWriter writer = new SpecificDatumWriter<AvroFlumeEvent>(AvroFlumeEvent.class);
    private boolean useAvroEvenFormat = false;

    private Map<CharSequence, CharSequence> toCharSeqMap(Map<String, String> map) {
        Map<CharSequence, CharSequence> rst = new HashMap<CharSequence, CharSequence>();
        for (Map.Entry<String, String> en : map.entrySet()) {
            rst.put(en.getKey(), en.getValue());
        }
        return rst;
    }


    @Override
    public Status process() throws EventDeliveryException {
        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = null;
        Event event = null;
        String eventTopic = null;
        String eventKey = null;

        try {
            long processedEvents = 0;

            transaction = channel.getTransaction();
            transaction.begin();

            messageList.clear();
            for (; processedEvents < batchSize; processedEvents += 1) {
                event = channel.take();

                if (event == null) {
                    // no events available in channel
                    break;
                }

                byte[] eventBody = event.getBody();
                Map<String, String> headers = event.getHeaders();

                if ((eventTopic = headers.get(TOPIC_HDR)) == null) {
                    eventTopic = topic;
                }

                eventKey = headers.get(KEY_HDR);

                if (eventKey == null) {
                    eventKey = getpartition(event) + "";
                } else {
                    logger.info("using header topic");
                }

                if (logger.isDebugEnabled()) {
                    logger.debug("{Event} " + eventTopic + " : " + eventKey + " : "
                            + new String(eventBody, "UTF-8"));
                    logger.debug("event #{}", processedEvents);
                }

                //include header
                if (useAvroEvenFormat) {
                    otstream.reset();
                    AvroFlumeEvent avroFlumeEvent = new AvroFlumeEvent(toCharSeqMap(headers), ByteBuffer.wrap(eventBody));
                    encoder = EncoderFactory.get().directBinaryEncoder(otstream, encoder);
                    writer.write(avroFlumeEvent, encoder);
                    encoder.flush();
                    eventBody = otstream.toByteArray();
                }

                // create a message and add to buffer
                KeyedMessage<String, byte[]> data = new KeyedMessage<String, byte[]>
                        (eventTopic, eventKey, eventBody);
                messageList.add(data);

            }

            // publish batch and commit.
            if (processedEvents > 0) {
                long startTime = System.nanoTime();
                producer.send(messageList);
                long endTime = System.nanoTime();
                counter.addToKafkaEventSendTimer((endTime - startTime) / (1000 * 1000));
                counter.addToEventDrainSuccessCount(Long.valueOf(messageList.size()));
            }

            transaction.commit();

        } catch (Exception ex) {
            String errorMsg = "Failed to publish events";
            logger.error("Failed to publish events", ex);
            result = Status.BACKOFF;
            if (transaction != null) {
                try {
                    transaction.rollback();
                    counter.incrementRollbackCount();
                } catch (Exception e) {
                    logger.error("Transaction rollback failed", e);
                    throw Throwables.propagate(e);
                }
            }
            throw new EventDeliveryException(errorMsg, ex);
        } finally {
            if (transaction != null) {
                transaction.close();
            }
        }

        return result;
    }

    @Override
    public synchronized void start() {
        // instantiate the producer
        ProducerConfig config = new ProducerConfig(kafkaProps);
        producer = new Producer<String, byte[]>(config);
        counter.start();
        super.start();
    }


    /**
     * need unregist configfile change listenner?
     */


    @Override
    public synchronized void stop() {
        producer.close();
        counter.stop();
        logger.info("Kafka Sink {} stopped. Metrics: {}", getName(), counter);
        super.stop();
    }


    /**
     * We configure the sink and generate properties for the Kafka Producer
     * <p>
     * Kafka producer properties is generated as follows:
     * 1. We generate a properties object with some static defaults that
     * can be overridden by Sink configuration
     * 2. We add the configuration users added for Kafka (parameters starting
     * with .kafka. and must be valid Kafka Producer properties
     * 3. We add the sink's documented parameters which can override other
     * properties
     *
     * @param context
     */
    @Override
    public void configure(Context context) {

        useAvroEvenFormat = context.getBoolean("useFlumeEventFormat", false);

        batchSize = context.getInteger(KafkaSinkConstants.BATCH_SIZE,
                KafkaSinkConstants.DEFAULT_BATCH_SIZE);
        messageList =
                new ArrayList<KeyedMessage<String, byte[]>>(batchSize);
        logger.debug("Using batch size: {}", batchSize);

        topic = context.getString(KafkaSinkConstants.TOPIC,
                KafkaSinkConstants.DEFAULT_TOPIC);
        if (topic.equals(KafkaSinkConstants.DEFAULT_TOPIC)) {
            logger.warn("The Property 'topic' is not set. " +
                    "Using the default topic name: " +
                    KafkaSinkConstants.DEFAULT_TOPIC);
        } else {
            logger.info("Using the static topic: " + topic +
                    " this may be over-ridden by event headers");
        }

        kafkaProps = KafkaSinkUtil.getKafkaProperties(context);

        if (logger.isDebugEnabled()) {
            logger.debug("Kafka producer properties: " + kafkaProps);
        }

        if (counter == null) {
            counter = new KafkaSinkCounter(getName());
        }

        if (MyMessagePartitionerOld.class.getName().equals(kafkaProps.get("partitioner.class"))) {
            logger.info("using mypartionner");
            loadpartionmap();
        }

    }


    Map<String, Object> partionmap = new HashMap<>();
    public int defaul_part = -1;//random..load balence by app

    public final String configfilename = "partitionmap.properties";
    private File configfile = null;

    /**
     * 加载配置文件
     */

    public void loadpartionmap() {
        partionmap.clear();
        defaul_part = -1;
        Properties properties = new Properties();

        try {
            if (configfile == null) {
                String[] configlocation = {".", "conf", "config", "src" + File.separator + "main" + File.separator + "resources"};
                File f = null;
                for (String dir : configlocation) {
                    f = new File(dir + File.separator + configfilename);
                    if (f.exists()) {
                        break;
                    }
                }
                if (!f.exists()) {
                    //no config file find
                    //use balence mod
                    logger.warn("no config file founded,current dir " + new File(".").getAbsolutePath().toString() + ", looking dirs" + configlocation
                            + ",looking for file " + configfilename + "\nusing app balance mode");

                    return;
                }
                logger.info("using configfile" + f.getAbsolutePath());
                configfile = f;
            }

            properties.load(new FileInputStream(configfile));
            Map<String, Object> tmap;
            for (Map.Entry en : properties.entrySet()) {

                String key = (String) en.getKey();

                if (key.equals("default")) {
                    if (!en.getValue().equals("random")) {
                        defaul_part = Integer.parseInt(en.getValue().toString());
                    }
                    continue;
                }

                tmap = partionmap;
                String[] sks = key.split("\\.");
                for (int i = 0; i < sks.length - 1; i++) {
                    String sk = sks[i];
                    if (tmap.containsKey(sk)) {

                        tmap = (Map<String, Object>) tmap.get(sk);
                    } else {
                        Map<String, Object> ttmap = new HashMap<>();
                        tmap.put(sk, ttmap);
                        tmap = (Map<String, Object>) tmap.get(sk);
                    }
                }
                tmap.put(sks[sks.length - 1], en.getValue());

            }
            logger.debug(partionmap.toString());
            logger.debug(defaul_part + "");


            watchconfig();

        } catch (Exception e) {
            defaul_part=-1;
            partionmap.clear();
            logger.error("load parttion map error,using balence mode ",e);
        }

    }

    private boolean watchstarted = false;

    /**
     * 起动线程监视配置文件
     */
    synchronized private void watchconfig() {
        if (watchstarted) {
            logger.warn("watching thread already started.");
            return;
        }
        watchstarted = true;
        logger.info("starting watch thread");
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    WatchService watcher = FileSystems.getDefault().newWatchService();
                    logger.info("watching dir " + configfile.getParent());
                    Path p = FileSystems.getDefault().getPath(configfile.getParent());
                    WatchKey key = p.register(watcher, StandardWatchEventKinds.ENTRY_MODIFY);
                    while (true) {
                        try {
                            key = watcher.take();

                            for (WatchEvent event : key.pollEvents()) {
                                if (event.kind() == StandardWatchEventKinds.ENTRY_MODIFY) {
                                    Path cp = (Path) event.context();
                                    if (cp.getFileName().toString().equals(configfilename)) {
                                        logger.info("config file changed will prepare to reload..");
                                        configchanged = true;
                                        break;
                                    }
                                    //logger.debug(event+":"+cp.getFileName());
                                }
                            }
                            if (!key.reset()) {
                                //may be need re regist
                                break;
                            }

                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        });
        t.setDaemon(true);
        t.start();
    }


    private static int toPositive(int number) {
        return number & 0x7fffffff;
    }

    /**
     * @param app
     * @param ip
     * @return -1 or bigger than partion max,shuold handle this
     */

    volatile boolean configchanged = false;


    /**
     * 通过event获取分区
     *
     * @param event
     * @return
     */

    private int getpartition(Event event) {
        int p = getpartition(event.getHeaders().get("application"), event.getHeaders().get("hostIP"));
        logger.debug(p + " : " + event.getHeaders());
        return p;

    }


    /**
     * 根据配置文件分配partition
     * 在本进程里重新加载配置，不用每次都lock
     *
     * @param app
     * @param ip
     * @return 一个正数，需要%分区数
     */
    private int getpartition(String app, String ip) {
        if (configchanged) {
            loadpartionmap();
            configchanged = false;
        }
        int rst = defaul_part;
        for (Object rule : partionmap.values()) {
            Map<String, String> m = (Map<String, String>) rule;
            if (m.get("app").equals("*") || m.get("app").equals(app)) {
                if (m.get("ip").equals("*") || m.get("ip").equals(ip)) {
                    rst = Integer.parseInt(m.get("partition").toString());
                    break;
                }
            }
        }

        if (rst < 0) {
            /*按app进行load balance,缓存增加性能？*/
            if (app == null) app = "default app";
            return toPositive(app.hashCode());
        }


        return rst;
    }


    /**
     * for junit test
     *
     * @param app
     * @param ip
     * @return
     */
    public int partitiontest(String app, String ip) {
        int numPartitions = 2;
        int p = getpartition(app, ip);
        if (p >= numPartitions) {
            return p % numPartitions;
        } else {
            return p;
        }
    }


}
