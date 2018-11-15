# OBI Predictor

OBI Predictor implements the Machine Learning capabilities built-in into OBI
and used as already explained in the main README file of this repository.
Despite already having a few predictor models capable of predicting the jobs
time duration for our specific models, this feature can be extended to other
types of jobs.

In order to implement a new predictive model, a new Python class extending the
abstract class `generic_predictor.GenericPredictor` should be added
to the `predictors` folder. This class must implement a `predict` method, which
should return the predicted job duration expressed in seconds.

OBI Predictor, in order to understand which predictor to pick when a job is
submitted, uses two different matching approaches:

 - **Filename based**: it tries to match the submitted job file name to a list
 of known file names for each type
 - **Source code based**: it tries to match the content of the submitted job
 source code to the code of a list of already available files.
 
To implement the above predictor name inference capabilities for any new model,
the user should add a field to the `predictor_matchers` dictionary available in 
the `predictor_utils.py` file following this style:

```python
'PREDICTOR_NAME': {
    'file_names': ['FILENAME_1', 'FILENAME_2'],
    'source_code': ['SOURCE_CODE1', 'SOURCE_CODE2']
}
```

It is up to the user to make the predictor able to find the `SOURCE_CODE*` files
in their correct location.

Along with the above information, the user should also "register" the newly 
added predictor by adding it to the `_DURATION_PREDICTORS` available in the
`predictors/__init__.py` file using as key `'PREDICTOR_NAME'` and as value an
instance of the predictor class.