

package de.tuberlin.dima.bdapro.flink.ml;

import java.io.File;

public class Config {
  private static final String PARENT_DIR = (new File(System.getProperty("user.dir"))).getParent();
  private static final String DATASET_INPUT_PATH = PARENT_DIR + "/dataset_ml/";
  private static final String OUTPUT_PATH = "";

  private Config() {}

  public static String pathToClassificationTrainingSet() {
    return DATASET_INPUT_PATH + "classification/train_noheader.tsv";
  }

//  public static String pathToTestSet() {
//    return INPUT_PATH + "test.tab";
//  }
//
//  public static String pathToOutput() {
//    return OUTPUT_PATH + "result";
//  }
//
//  public static String pathToSums() {
//    return OUTPUT_PATH + "sums";
//  }
//
//  public static String pathToConditionals() {
//    return OUTPUT_PATH + "conditionals";
//  }
//
//  public static double getSmoothingParameter() {
//    return SMOOTHING_PARAM;
//  }
//
//  public static String pathToSecretTestSet() {
//    return INPUT_PATH + "secrettest.dat";
//  }
//
//  public static String pathToSecretOutput() {
//    return OUTPUT_PATH + "secretresult";
//  }

}
