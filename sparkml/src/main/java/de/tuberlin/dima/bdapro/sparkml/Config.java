
package de.tuberlin.dima.bdapro.sparkml;

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
