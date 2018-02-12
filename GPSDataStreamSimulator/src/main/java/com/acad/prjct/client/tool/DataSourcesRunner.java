/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.acad.prjct.client.tool;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DataSourcesRunner {
  private static final Logger LOG = Logger.getLogger(DataSourcesRunner.class.getName());

  public static void main(@SuppressWarnings("ParameterCanBeLocal") String[] args) {
    args = new String[]{"D:\\data_dump\\Geolife Trajectories 1.3\\Data",
      "192.168.56.106:9092", "raw-gpstrajectory-data-topic"};
    // real name for topic
    //noinspection ConstantConditions
    if (args.length < 3) {
      System.err.println("General argument: <data_dir> <broker_url> <topic>");
      System.exit(-1);
    }

    File[] files = new File(args[0])
      .listFiles((File dir, String name) -> name.contains(".pltdata"));
    assert files != null;
    LOG.log(Level.INFO, "Files found \n{0}", Arrays.asList(files));
    int noThreads = files.length;
    LOG.log(Level.INFO, "setting the number of concurrent threads to {0}", noThreads);
    Executor executor = Executors.newFixedThreadPool(noThreads);
    for (File file : files) {
      executor.execute(new SeparateDataSourceSimulator(file, args[1], args[2]));
    }
  }
}
