package de.tuberlin.dima.bdapro.flinkml.classification

import de.tuberlin.dima.bdapro.flinkml.Config
import org.apache.flink.api.scala._
import org.apache.flink.ml.RichExecutionEnvironment
import org.apache.flink.ml.classification.SVM
import org.apache.flink.ml.common.LabeledVector

/**
  * Created by seema on 13.08.17.
  */
class SVMClassification(val envPassed : ExecutionEnvironment) {
  val env = envPassed

  def execute() : Double = {

//    val params: ParameterTool = ParameterTool.fromArgs(args)
//    val env = ExecutionEnvironment.getExecutionEnvironment
//    env.getConfig.setGlobalJobParameters(params)

    val pathToDataset = Config.pathToClassificationTrainingSet
    // Read the training data set, from a LibSVM formatted file
    val trainingDS: DataSet[LabeledVector] = env.readLibSVM(pathToDataset)

    // Create the SVM learner
    val svm = SVM()
      .setBlocks(10).setOutputDecisionFunction(false)

    // Learn the SVM model
    svm.fit(trainingDS)
    // Read the testing data set
    val testingDS: DataSet[LabeledVector] = env.readLibSVM(pathToDataset)
    val evaluationDS: DataSet[(Double, Double)] = svm.evaluate(testingDS.map(x => (x.vector, x.label)))
    //evaluationDS.print()

    val count = evaluationDS.count()
    val accuracy = evaluationDS .collect().map{
      case (pred, label) => if (pred == label) 1.0 else 0.0}.sum

    return (accuracy * 100.0 / count )

  }

}
