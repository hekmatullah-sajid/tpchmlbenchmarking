
package de.tuberlin.dima.bdapro.sparkml;

/**
 * <p>
 * The Config class is used to configure the path to directory where data
 * sets are located and define functions to get the intended data set path for executing an algorithm.
 * </p>
 * 
 * @author Hekmatullah Sajid and Seema Narasimha Swamy
 */
public class Config {
	private static String PARENT_DIR;

	private Config() {
	}

	public static void SetBaseDir(String path) {
		PARENT_DIR = path;
	}

	public static String pathToClassificationTrainingSet() {
		return PARENT_DIR + "classification.data";
	}
	
	public static String pathToNaiveBayesClassificationTrainingSet() {
		return PARENT_DIR + "naivenayes.data";
	}

	public static String pathToRecommendationTrainingSet() {
		return PARENT_DIR + "recommendation.data";
	}

	public static String pathToClusteringTrainingSet() {
		return PARENT_DIR + "classification.data";
	}

	public static String pathToRegressionTrainingSet() {
		return PARENT_DIR + "regression.data";
	}

}
