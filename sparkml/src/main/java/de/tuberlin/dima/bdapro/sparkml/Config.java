

package de.tuberlin.dima.bdapro.sparkml;


public class Config {
  private static String PARENT_DIR;

  private Config() {}
  public static void SetBaseDir(String path)
  {
    PARENT_DIR = path;
  }

  public static String pathToClassificationTrainingSet() {return PARENT_DIR + "/classification/phishing.txt"; }

  public static String pathToRecommendationTrainingSet() {
    return PARENT_DIR + "/recommendation/u.data";
  }

  public static String pathToClusteringTrainingSet() {return PARENT_DIR + "/clustering/sample_kmeans_data.txt"; }

  public static String pathToRegressionTrainingSet() {
    return PARENT_DIR + "/regression/mpg.txt";
  }


}
