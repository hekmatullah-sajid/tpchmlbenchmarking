

package de.tuberlin.dima.bdapro.sparkml;

import java.io.File;

public class Config {
  private static final String PARENT_DIR = (new File(System.getProperty("user.dir"))).getParent();
  private static final String DATASET_INPUT_PATH = PARENT_DIR + "/tpchmlbenchmarking/dataset_ml/";
  private static final String OUTPUT_PATH = "";

  private Config() {}

  public static String pathToClassificationTrainingSet() {return DATASET_INPUT_PATH + "classification/phishing.txt"; }

  public static String pathToRecommendationrainingSet() {
    return DATASET_INPUT_PATH + "recommendation/u.data";
  }

  public static String pathToClusteringTrainingSet() {
    return DATASET_INPUT_PATH + "clustering/phishing.txt";
  }

  public static String pathToRegressionTrainingSet() {
    return DATASET_INPUT_PATH + "regression/phishing.txt";
  }


}
