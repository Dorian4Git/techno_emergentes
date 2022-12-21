# techno_emergentes

- Presentation made in class is there under preentation.pptx
- The csv we used for the project
- We can find 3 scripts:
  - kafka producer: Get the data from the csv file
  - kafka consumer: Consume data sent by producer, cast it, so we can make analysis
  - spark scripts with hive support

## Modèle de prédiction
### Accuracy
The accuracy of the model is calculated using a binary classification evaluator. The binary classification evaluator takes in the label and prediction columns of the predictions dataframe as input, and then calculates a metric based on those predictions. In this case, the evaluator calculates the "areaUnderROC" metric, which represents the area under the receiver operating characteristic curve. This curve plots the true positive rate (sensitivity) against the false positive rate (1 - specificity) for different classification thresholds. The area under this curve is a measure of the model's accuracy, with a value of 0.5 representing a random classifier and a value of 1.0 representing a perfect classifier.

The accuracy of the model is then calculated by calling the evaluate method of the binary classification evaluator and passing in the predictions dataframe and the metric to be calculated as arguments. The resulting value is then stored in the accuracy variable and printed out.
