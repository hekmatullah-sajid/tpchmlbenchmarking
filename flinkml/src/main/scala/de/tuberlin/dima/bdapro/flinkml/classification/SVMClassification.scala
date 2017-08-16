package scala.de.tuberlin.dima.bdapro.flinkml.classification

import de.tuberlin.dima.bdapro.flinkml.Config
import org.apache.flink.api.scala._
import org.apache.flink.ml.RichExecutionEnvironment
import org.apache.flink.ml.classification.SVM
import org.apache.flink.ml.common.LabeledVector

/**
  * Created by seema on 13.08.17.
  */
class SVMClassification(val env : ExecutionEnvironment) {
  val localenv: ExecutionEnvironment = env

  def execute() {

//    val params: ParameterTool = ParameterTool.fromArgs(args)
//    val env = ExecutionEnvironment.getExecutionEnvironment
//    env.getConfig.setGlobalJobParameters(params)

    val pathToTrainingFile = Config.pathToClassificationTrainingSet
    val pathToTestingFile = Config.pathToClassificationTrainingSet
    // Read the training data set, from a LibSVM formatted file
    val trainingDS: DataSet[LabeledVector] = localenv.readLibSVM(pathToTrainingFile)

    // Create the SVM learner
    val svm = SVM()
      .setBlocks(10).setOutputDecisionFunction(false)

    // Learn the SVM model
    svm.fit(trainingDS)
    // Read the testing data set
    val testingDS: DataSet[LabeledVector] = env.readLibSVM(pathToTestingFile)
    val evaluationDS: DataSet[(Double, Double)] = svm.evaluate(testingDS.map(x => (x.vector, x.label)))
    //evaluationDS.print()

    val count = evaluationDS.count()
    val accuracy = evaluationDS .collect().map{
      case (pred, label) => if (pred == label) 1.0 else 0.0}.sum

    print(accuracy * 100.0 / count )

  }

}
