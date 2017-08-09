//package de.tuberlin.dima.bdapro.flink.ml.classification;
//
//import de.tuberlin.dima.bdapro.flink.ml.Config;
//import org.apache.flink.api.common.functions.FlatMapFunction;
//import org.apache.flink.api.java.DataSet;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.api.java.operators.DataSource;
//import org.apache.flink.api.java.tuple.Tuple1;
//import org.apache.flink.util.Collector;
//
//
//import org.apache.flink.ml.math.Vector;
//import org.apache.flink.ml.common.LabeledVector;
//import org.apache.flink.ml.classification.SVM;
//import org.apache.flink.ml.MLUtils;
//
///**
// * Created by seema on 27.07.17.
// */
//public class Training {
//    protected final ExecutionEnvironment env;
//
//    Training(ExecutionEnvironment env)
//    {
//        this.env = env;
//    }
//
//    void process()
//    {
//        String pathToTrainingFile = Config.pathToClassificationTrainingSet();
//        String pathToTestingFile = Config.pathToClassificationTrainingSet();
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//
//        // Read the training data set, from a LibSVM formatted file
//        DataSet trainingDS = MLUtils.readLibSVM(env, pathToTrainingFile);
//        DataSet testingDS = env.readLibSVM(pathToTrainingFile);
//
//        // Create the SVM learner
//        SVM svm = new SVM()
//                .setBlocks(10);
//
//// Learn the SVM model
//        svm.fit(trainingDS);
//
//// Read the testing data set
//        DataSet testingDS = env.readLibSVM(pathToTestingFile).map(_.vector)
//
//// Calculate the predictions for the testing data set
//        DataSet predictionDS = svm.predict(testingDS);
//    }
//
//
//}
