package org.radargun.stages;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.radargun.reporting.ClusterReport;
import org.radargun.reporting.LineClusterReport;
import org.radargun.utils.Utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

/**
 * Stage that generates a chart from a set of csv files.
 * <pre>
 * - fnPrefix: the prefix of the generated chart file (png). No value by default, optional parameter.
 * - reportDirectory - where are the csv files located. Defaults to 'reports'
 * - outputDir - where to output the generated graphical reports. Defaults to 'reports'
 * </pre>
 *
 * @author Mircea.Markus@jboss.com
 */
public class GenerateChartStage extends AbstractMasterStage {

   private static Log log = LogFactory.getLog(GenerateChartStage.class);

   public static final String X_LABEL_CLUSTER_SIZE = "Cluster size (number of cache instances)";
   public static final String X_LABEL_ITERATION = "Iteration (related to number of stressor threads)";
   public static final String REPORTS = "reports";

   private String reportDirectory = REPORTS;
   private String csvFilesDirectory = "reports";
   private String fnPrefix;
   private Map<String, List<String>> filter = new HashMap<String, List<String>>();
   protected Map<String, List<Pattern>> compiledFilters = null;
   ClusterReport putReport = new LineClusterReport();
   ClusterReport getReport = new LineClusterReport();

   public boolean execute() throws Exception {
      File[] files = getFilteredFiles(new File(csvFilesDirectory));
      boolean xLabelIteration = true;
      for (File f : files) {
         try {
            StringTokenizer tokenizer = new StringTokenizer(Utils.fileName2Config(f.getName()), "_");
            String productName = tokenizer.nextToken();
            String configName = tokenizer.nextToken();
            String prefix = productName + "_" + configName;
            for (File other : files) {
               if (!other.getName().equals(f.getName()) && other.getName().startsWith(prefix)) {
                  xLabelIteration = false;
                  break;
               }
            }
         } catch (Throwable e) {            
         }
      }
      for (File f : files) {
         readData(f, xLabelIteration);
      }
           
      putReport.setReportFile(reportDirectory, fnPrefix + "_PUT");
      putReport.init(xLabelIteration ? X_LABEL_ITERATION : X_LABEL_CLUSTER_SIZE,
            "PUT ops/sec on each cache instance", "Average PUT per cache instance", getSubtitle());
      getReport.setReportFile(reportDirectory, fnPrefix + "_GET");
      getReport.init(xLabelIteration ? X_LABEL_ITERATION : X_LABEL_CLUSTER_SIZE,
            "GET ops/sec on each cache instance", "Average GET per cache instance", getSubtitle());
      
      putReport.generate();
      getReport.generate();
      return true;
   }

   private void readData(File f, boolean xLabelIteration) throws IOException {
      log.debug("Processing file " + f);
      //expected file format is: <product>_<config>_<size>.csv
      final String productName;
      final String configName;
      int clusterSize = 0;      
      try {
         StringTokenizer tokenizer = new StringTokenizer(Utils.fileName2Config(f.getName()), "_");
         productName = tokenizer.nextToken();
         configName = tokenizer.nextToken();
         clusterSize = Integer.parseInt(tokenizer.nextToken());         
      } catch (Throwable e) {
         String fileName = f == null ? null : f.getAbsolutePath();
         log.error("unexpected exception while parsing filename: " + fileName, e);
         return;
      }

      //now the contents of this file:
      String line;
      BufferedReader br = new BufferedReader(new FileReader(f));
      String header = br.readLine(); //this is the header
      String[] columns = header.split(",", -1);
      int getIndex = getIndexOf("READS_PER_SEC", columns);
      int putIndex = getIndexOf("WRITES_PER_SEC", columns);
      int iterIndex = getIndexOf("ITERATION", columns);
      if (getIndex < 0 || putIndex < 0) {
         log.error("Cannot find statistics in file " + f.getAbsolutePath());
         br.close();
         return;
      }
      Map<String, Double> avgPutsPerSec = new HashMap<String, Double>();
      Map<String, Double> avgGetsPerSec = new HashMap<String, Double>();
      while ((line = br.readLine()) != null) {
         Stats s = getAveragePutAndGet(line, iterIndex, getIndex, putIndex);
         log.debug("Read stats " + s);
         if (s != null) {
            Double avgPut = avgPutsPerSec.get(s.iteration);
            avgPutsPerSec.put(s.iteration, avgPut == null ? s.putsPerSec : s.putsPerSec + avgPut);
            Double avgGet = avgGetsPerSec.get(s.iteration);
            avgGetsPerSec.put(s.iteration,  avgGet == null ? s.getsPerSec : s.getsPerSec + avgGet);
         }
      }
      br.close();
      
      for (String iteration : avgPutsPerSec.keySet()) {
         String name = productName + "(" + configName + ")" + (xLabelIteration ? "" : iteration);
         
         Double avgPut = avgPutsPerSec.get(iteration);
         Double avgGet = avgGetsPerSec.get(iteration);
         
         int xCoord;
         if (xLabelIteration) {
            try {
               xCoord = Integer.parseInt(iteration);
            } catch (NumberFormatException e) {
               xCoord = clusterSize;
            }
         } else {
            xCoord = clusterSize;
         }
         putReport.addCategory(name, xCoord, avgPut / clusterSize);
         getReport.addCategory(name, xCoord, avgGet / clusterSize);
      }
   }

   private int getIndexOf(String string, String[] parts) {
      int i = 0;
      for (String part : parts) {
         if (part.equals(string)) return i;
         ++i;
      }
      return -1;
   }

   private Stats getAveragePutAndGet(String line, int iterIndex, int getIndex, int putIndex) {
      // To be a valid line, the line should be comma delimited
      String[] parts = line.split(",", -1);
      String getStr = parts[getIndex];
      String putStr = parts[putIndex];
      
      Stats s = new Stats();
      try {
         s.putsPerSec = putStr.isEmpty() ? 0 : Double.parseDouble(putStr);
         s.getsPerSec = getStr.isEmpty() ? 0 : Double.parseDouble(getStr);
      }
      catch (NumberFormatException nfe) {
         log.error("Unable to parse file properly!", nfe);
         return null;
      }
      s.iteration = iterIndex < 0 ? "" : parts[iterIndex];      
      return s;
   }


   public void setCsvFilesDirectory(String csvFilesDirectory) {
      this.csvFilesDirectory = csvFilesDirectory;
   }

   public void setFnPrefix(String fnPrefix) {
      this.fnPrefix = fnPrefix;
   }

   public void setReportDirectory(String reportDirectory) {
      this.reportDirectory = reportDirectory;
   }

   protected File[] getFilteredFiles(File file) {
      return file.listFiles(new FilenameFilter() {
         //accepted file names are <product-name>_<config-name>_<cluster-size>.csv
         public boolean accept(File dir, String name) {
            compileFilters();
            if (!name.toUpperCase().endsWith(".CSV")) {
               return false;
            }
            if (!isUsingFilters()) {
               return true;
            }
            StringTokenizer tokenizer = new StringTokenizer(name, "_");
            String productName = tokenizer.nextToken();
            String configName = tokenizer.nextToken();
            if (!filter.containsKey(productName)) return false;

            //first check that this works as compilation issues might have occurred during parsing the patterns
            if (filter.get(productName).contains(configName)) {
               return true;
            } else {
               if (configName.equals("*")) {
                  return true;
               } else {
                  List<Pattern> patternList = compiledFilters.get(productName);
                  for (Pattern pattern : patternList) {
                     if (pattern.matcher(configName).find()) {
                        log.trace("Pattern '" + pattern + "' matched config: " + configName);
                        return true;
                     }
                  }
               }
            }
            return false;
         }
      });
   }

   private void compileFilters() {
      if (compiledFilters == null) {
         compiledFilters = new HashMap<String, List<Pattern>>();
         for (String product : filter.keySet()) {
            List<Pattern> compiled = new ArrayList<Pattern>();
            for (String aFilter : filter.get(product)) {
               if (aFilter.equals("*") || aFilter.equals("?") || aFilter.equals("+")) {
                  String oldPatter = aFilter;
                  aFilter = "[" + aFilter + "]";
                  log.info("Converting the pattern from '" + oldPatter + "' to '" + aFilter +"'. " +
                        "See: http://arunma.com/2007/08/23/javautilregexpatternsyntaxexception-dangling-meta-character-near-index-0");
               }
               try {
                  Pattern pattern = Pattern.compile(aFilter);
                  compiled.add(pattern);
               } catch (Exception e) {
                  String message = "Exception while compiling the pattern: '" + aFilter + "'";
                  if (e.getMessage().indexOf("Dangling meta character ") >= 0) {
                     message += "If your regexp is like '*' or '+' (or other methachars), add square brackets to it, " +
                           "e.g. '[*]'. See: http://arunma.com/2007/08/23/javautilregexpatternsyntaxexception-dangling-meta-character-near-index-0/";
                  } else {
                  }
                  log.warn(message, e);

               }
            }
            compiledFilters.put(product, compiled);
         }
      }
   }

   public boolean isUsingFilters() {
      return !filter.isEmpty();
   }


   public void addReportFilter(String prodName, String config) {
      List<String> stringList = filter.get(prodName);
      if (stringList == null) {
         stringList = new ArrayList<String>();
         filter.put(prodName, stringList);
      }
      stringList.add(Utils.fileName2Config(config));
   }

   public static String getSubtitle() {
      return "Generated on " + new Date() + " by RadarGun\nJDK: " +
            System.getProperty("java.vm.name") + " (" + System.getProperty("java.vm.version") + ", " +
            System.getProperty("java.vm.vendor") + ") OS: " + System.getProperty("os.name") + " (" +
            System.getProperty("os.version") + ", " + System.getProperty("os.arch") + ")";
   }

   private static class Stats {
      private double putsPerSec, getsPerSec;
      private String iteration;

      public String toString() {
         return "Stats{" +
               "avgPut=" + putsPerSec +
               ", avgGet=" + getsPerSec +
               '}';
      }
   }
}
