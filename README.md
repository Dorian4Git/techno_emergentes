# techno_emergentes

- Presentation made in class is there under preentation.pptx
- The csv we used for the project
- We can find 3 scripts:
  - kafka producer: Get the data from the csv file
  - kafka consumer: Consume data sent by producer, cast it, so we can make analysis
  - spark scripts with hive support

## Modèle de prédiction
### Training set, tests data
In machine learning, it is common to split the data into a training set and a test set. The training set is used to train the model, while the test set is used to evaluate the model's performance on unseen data. This process helps to ensure that the model is able to generalize to new data, rather than just memorizing the training data.

In the code you provided, the data is split into a training set and a test set using the randomSplit method of the dataframe. This method takes in two arguments: the proportions of the data to be split into the training and test sets, and a seed value for the random number generator. The proportions are specified as a list, with the first element being the proportion of the data to be used for training and the second element being the proportion of the data to be used for testing. In this case, the data is split into a 70/30 training/test split.

Once the data has been split into a training and test set, the training set is used to fit the model using the fit method of the logistic regression object. The test set is then used to make predictions on the model using the transform method. The resulting predictions are then evaluated using the binary classification evaluator to calculate the model's accuracy.

### Evaluator
In the code you provided, the evaluator is a binary classification evaluator. A binary classification evaluator is a tool that is used to evaluate the performance of a binary classification model, which is a type of machine learning model that is used to predict a binary outcome (e.g., true or false, 0 or 1).

The evaluator takes in the label and prediction columns of the predictions dataframe as input, and then calculates a metric based on those predictions. The metric used in this case is the "areaUnderROC" metric, which represents the area under the receiver operating characteristic curve. This curve plots the true positive rate (sensitivity) against the false positive rate (1 - specificity) for different classification thresholds. The area under this curve is a measure of the model's accuracy, with a value of 0.5 representing a random classifier and a value of 1.0 representing a perfect classifier.

The evaluator is then used to calculate the accuracy of the model by calling the evaluate method of the evaluator and passing in the predictions dataframe and the metric to be calculated as arguments. The resulting value is then stored in the accuracy variable and printed out.

### Accuracy
The accuracy of the model is calculated using a binary classification evaluator. The binary classification evaluator takes in the label and prediction columns of the predictions dataframe as input, and then calculates a metric based on those predictions. In this case, the evaluator calculates the "areaUnderROC" metric, which represents the area under the receiver operating characteristic curve. This curve plots the true positive rate (sensitivity) against the false positive rate (1 - specificity) for different classification thresholds. The area under this curve is a measure of the model's accuracy, with a value of 0.5 representing a random classifier and a value of 1.0 representing a perfect classifier.

The accuracy of the model is then calculated by calling the evaluate method of the binary classification evaluator and passing in the predictions dataframe and the metric to be calculated as arguments. The resulting value is then stored in the accuracy variable and printed out.
