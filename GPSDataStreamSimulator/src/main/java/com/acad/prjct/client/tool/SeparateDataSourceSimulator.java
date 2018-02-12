
package com.acad.prjct.client.tool;

import com.acad.prjct.client.tool.kafka.KafkaMessageProducer;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

public class SeparateDataSourceSimulator implements Runnable {
  private final File file;
  private final String topic;
  private final KafkaMessageProducer kafkaMessageProducer;
  private final Random random;
  private static final Logger LOG = Logger.getLogger(SeparateDataSourceSimulator.class.getName()
    + Thread.currentThread().getName());

  SeparateDataSourceSimulator(File file, String brokerUrl, String topic) {
    this.file = file;
    this.topic = topic;
    this.kafkaMessageProducer = new KafkaMessageProducer(brokerUrl);
    this.random = new Random(1234);
    LOG.log(Level.INFO, "Thread worker initialized to read from {0}", file);
  }

  @Override
  public void run() {
    //read file content
    LineIterator lIt;
    try {
      LOG.log(Level.INFO, "Reading the content of file");
      lIt = FileUtils.lineIterator(this.file);
    } catch (IOException ex) {
      Logger.getLogger(SeparateDataSourceSimulator.class.getName()).log(Level.SEVERE, null, ex);
      throw new RuntimeException(ex);
    }
    String line;
    LOG.log(Level.INFO, "Sending file content to kafka topic - {0}", this.topic);
    AtomicInteger i = new AtomicInteger(0);
    while (lIt.hasNext()) {
      line = lIt.nextLine();
      //send message to kafka
      LOG.log(Level.INFO, "inside loop before send ");
      this.kafkaMessageProducer.send(this.topic, line);
      LOG.log(Level.INFO, "inside loop ");
      if (i.incrementAndGet() % 5 == 0) {
        LOG.log(Level.INFO, "sending lines in progress. " + "Check point: " + i);
      }
      try {
        //noinspection AccessStaticViaInstance
        Thread.currentThread().sleep(this.random.nextInt(222));
      } catch (InterruptedException ex) {
        Logger.getLogger(SeparateDataSourceSimulator.class.getName()).log(Level.SEVERE, null, ex);
      }
    }
  }
}
