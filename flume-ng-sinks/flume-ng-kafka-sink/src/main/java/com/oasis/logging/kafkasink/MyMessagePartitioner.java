package com.oasis.logging.kafkasink;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by BG246070 on 2016/8/3.
 */
public class MyMessagePartitioner implements Partitioner {


    Logger logger = Logger.getLogger(MyMessagePartitioner.class);

    //default partitioner
    //org.apache.kafka.clients.producer.internals.DefaultPartitioner defaultPartitioner;


    private static int toPositive(int number) {
        return number & 0x7fffffff;
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
        } else if (p < 0) {
            /*缓存增加性能？*/
            return toPositive(app.hashCode()) % numPartitions;
        } else {
            return p;
        }
    }


    private BinaryDecoder decoder;
    private static SpecificDatumReader<AvroFlumeEvent> reader = new SpecificDatumReader<AvroFlumeEvent>(AvroFlumeEvent.class);

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        String app = "defaultapp";
        String ip = "127.0.0.1";

        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();

        AvroFlumeEvent event = null;
        if (value instanceof AvroFlumeEvent) {
            event = (AvroFlumeEvent) value;
            logger.debug("no need to decode");
        } else {
            logger.debug("need to decode,value is " + value);
            ByteArrayInputStream in =
                    new ByteArrayInputStream(valueBytes);
            decoder = DecoderFactory.get().directBinaryDecoder(in, decoder);
            try {
                event = reader.read(null, decoder);
            } catch (IOException e) {
                logger.error("can't resolve event!");
                e.printStackTrace();
            }
        }

        if (event != null) {
            app = event.getHeaders().get("application").toString();
            ip = event.getHeaders().get("hostIP").toString();
        }


//        if (key instanceof String) {
//            int c = Integer.parseInt((String) key);
//            if (c % 3 == 0) {
//                app = "testflume";
//            } else if (c % 3 == 1) {
//                app = "testapp";
//            }
//        }
        int p = getpartition(app, ip);
        logger.debug(app + p + "/" + numPartitions);
        if (p >= numPartitions) {
            return p % numPartitions;
        } else if (p < 0) {
            /*缓存增加性能？*/
            return toPositive(app.hashCode()) % numPartitions;
        } else {
            return p;
        }
    }

    /**
     * @param app
     * @param ip
     * @return -1 or bigger than partion max,shuold handle this
     */

    volatile boolean configchanged = false;

    /**
     * 根据配置文件分配partition
     * 在本进程里重新加载配置，不用每次都lock
     *
     * @param app
     * @param ip
     * @return
     */
    private int getpartition(String app, String ip) {
        if (configchanged) {
            loadpartionmap();
            configchanged = false;
        }
        for (Object rule : partionmap.values()) {
            Map<String, String> m = (Map<String, String>) rule;
            if (m.get("app").equals("*") || m.get("app").equals(app)) {
                if (m.get("ip").equals("*") || m.get("ip").equals(ip)) {
                    return Integer.parseInt(m.get("partition").toString());
                }
            }
        }
        return defaul_part;
    }

    /**
     * need unregist configfile change listenner?
     */

    @Override
    public void close() {


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

            String[] configlocation = {".", "conf", "config", "src" + File.separator + "main" + File.separator + "resources"};
            File f = null;
            for (String dir : configlocation) {
                f = new File(dir + File.separator + configfilename);
                if (f.exists()) {
                    break;
                }
            }
            if (f == null) {
                //no config file find
                //use balence mod
                logger.warn("no config file founded,current dir " + new File(".").getAbsolutePath().toString() + ", looking dirs" + configlocation
                        + ",looking for file " + configfilename + "\nusing app balance mode");

                return;
            }
            logger.info("using configfile" + f.getAbsolutePath());
            configfile = f;
            properties.load(new FileInputStream(f));
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
            logger.debug(partionmap);
            logger.debug(defaul_part);


            watchconfig();

        } catch (IOException e) {
            e.printStackTrace();
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
                    Path p = FileSystems.getDefault().getPath(configfile.getParent());
                    WatchKey key = p.register(watcher, StandardWatchEventKinds.ENTRY_MODIFY);
                    while (true) {
                        try {
                            key = watcher.take();

                            for (WatchEvent event : key.pollEvents()) {
                                if (event.kind() == StandardWatchEventKinds.ENTRY_MODIFY) {
                                    Path cp = (Path) event.context();
                                    if (cp.getFileName().equals(configfilename)) {
                                        logger.info("config file changed will prepare to reload..");
                                        configchanged = true;
                                    }
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

    @Override
    public void configure(Map<String, ?> configs) {
        loadpartionmap();
    }
}
