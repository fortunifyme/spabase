/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.acad.prjct.client.tool;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
public class FileMerger {
  private List<String> allContents;
  private static final Logger LOG = Logger.getLogger(FileMerger.class.getName());

  private FileMerger() {
    allContents = new ArrayList<>(100000);
  }

  public static void main(@SuppressWarnings("ParameterCanBeLocal") String[] args) {
    args = new String[]{"D:\\data_dump\\Geolife Trajectories 1.3\\Data_original", "000", "084",
      "D:\\data_dump\\Geolife Trajectories 1.3\\Data_all_one_file"};
    //noinspection ConstantConditions
    if (args.length < 4) {
      System.err.println("General argument: <data_dir> <start_folder> <end_folder> <outputDir>");
      System.exit(-1);
    }
    try {
      new FileMerger().begin(args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]), args[3]);
    } catch (IOException ex) {
      LOG.log(Level.SEVERE, null, ex);
    }
    LOG.log(Level.INFO, "* Process complete * ");
  }

  /**
   * @param folder    - the directory where the data dumps are located
   * @param start     - the dir with data dumps contain the folders which named by using 3 digits. So
   *                  the start correspond the beginning subfolder data dumps ( for more info please
   *                  reference to project description )
   * @param end       - by analogy to start
   * @param outputDir - self explanatory
   * @throws IOException - just in case
   */
  private void begin(String folder, int start, int end, String outputDir) throws IOException {
    if (start >= end) {
      throw new IllegalArgumentException("start and end folder can't be the same");
    }
    File dataFolder = new File(folder);
    String file;
    for (int i = start; i <= end; i++) {
      file = leftPadWithZeros(i, 3);
      LOG.log(Level.INFO, "reading file - {0}", file);
      readTrajectoryFolder(dataFolder, file);
    }
    LOG.log(Level.INFO, "sorting all content read into a new file");
    allContents.sort(new RecordComparator());
    file = leftPadWithZeros(start, 3) + "-" + leftPadWithZeros(end, 3) + ".pltdata";
    LOG.log(Level.INFO, "sorting all content read into a new file - {0}", file);
    Files.write(Paths.get(outputDir, file),
      allContents, StandardOpenOption.CREATE_NEW);
  }

  private void readTrajectoryFolder(File dataFolder, String userFolder) throws IOException {
    Stream<Path> files = Files.list(Paths.get(dataFolder.getAbsolutePath(), userFolder, "Trajectory"));
    files.forEach((Path path) -> {
      try {
        List<String> content = Files.lines(path).map((String line) -> userFolder + "," +
          path.getFileName().toString().substring(0, 14) + "," + line).collect(Collectors.toList());

        allContents.addAll(validateContent(content).subList(6, content.size()));
      } catch (IOException ex) {
        Logger.getLogger(FileMerger.class.getName()).log(Level.SEVERE, null, ex);
      }
    });
  }

  private List<String> validateContent(List<String> rawContent) {
    return rawContent.stream().filter(x -> !x.contains("777")).collect(Collectors.toList());
  }

  private String leftPadWithZeros(int i, @SuppressWarnings("SameParameterValue") int len) {
    StringBuilder s = new StringBuilder(String.valueOf(i));
    if (s.length() < len) {
      for (int a = 0; a <= len - s.length(); a++) {
        s.insert(0, '0');
      }
    }
    return s.toString();
  }

  private class RecordComparator implements Comparator<String> {
    final DateFormat df = new SimpleDateFormat("yyyy-MM-dd,HH:mm:ss");

    @Override
    public int compare(String o1, String o2) {
      try {
        //000,39.976437,116.34093,0,306,39746.1958217593,2008-10-25,04:41:59
        String[] part1s = o1.split(",");
        Date date1 = df.parse(part1s[7] + "," + part1s[8]);
        String[] part2s = o2.split(",");
        Date date2 = df.parse(part2s[7] + "," + part2s[8]);
        return date1.compareTo(date2);
      } catch (ParseException ex) {
        Logger.getLogger(FileMerger.class.getName()).log(Level.SEVERE, null, ex);
      }
      return 0;
    }
  }
}
