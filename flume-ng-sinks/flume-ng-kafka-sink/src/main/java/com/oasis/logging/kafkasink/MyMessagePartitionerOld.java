package com.oasis.logging.kafkasink;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;

/**
 * Created by BG246070 on 2016/8/3.
 */
public class MyMessagePartitionerOld implements Partitioner {


    Logger logger = Logger.getLogger(MyMessagePartitionerOld.class);

    public MyMessagePartitionerOld(VerifiableProperties props) {

    }

    /**
     * o is message key
     * i is number of partions
     * 因为0.8版本的分区类只能拿到key,但主机名，ip地址都在body里，所以分区的逻辑需要移动到sink里
     * @param o
     * @param i
     * @return
     */
    @Override
    public int partition(Object o, int i) {
        int rst = 0;
        try {
            rst = Integer.parseInt(o.toString()) % i;
            if (rst < 0) {
                logger.error("should not got negative number!");
                rst = 0 - i;
            }
        } catch (Exception e) {
            logger.error("faile to part key" + o);
        }
        return rst;
    }
}
