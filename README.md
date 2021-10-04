# National Diabetes Audit RAP project
## What is NDA RAP?
This repository contains the RAP NDA pipeline created for the quarterly publication of the [National Diabetes Audit programme](https://digital.nhs.uk/data-and-information/clinical-audits-and-registries/national-diabetes-audit), producing the Care Processes and Treatment Targets publication quarterly publication outputs. The pipeline has been designed following the [Reproducible Analytical Pipelines](https://gss.civilservice.gov.uk/reproducible-analytical-pipelines/) working practices and the principles of the [AQUA Book guidelines](https://www.gov.uk/government/publications/the-aqua-book-guidance-on-producing-quality-analysis-for-government).

__Disclaimer__: The current NDA RAP project is a proof of concept, designed to demonstrate the feasibility of applying RAP principles to existing NHS Digital publications. It is a work in progress with further updates planned for the future. 

This repository is owned and maintained by NHS Digital. Responsible statistician: Peter Knighton

## Links to Publication Website & Metadata
* [National Diabetes Audit publications](https://digital.nhs.uk/data-and-information/publications/statistical/national-diabetes-audit)
* [NDA past publications](https://digital.nhs.uk/data-and-information/publications/statistical/national-diabetes-audit#past-publications)
# Getting Started 
## Initial package set up
The current NDA RAP package is set up to run in Windows only. Run the following command in Terminal or VScode to set up the package. 

The code runs without errors, however access credentials to the database have been removed. Feel free to create your own test data to apply the code on.

```
python setup.py install
pip install -r requirements.txt
```
## Directory structure:
```
diabetes_rap
├───README.md
│
├───diabetes_code
│   ├─── create_publication.py
│   ├─── params.py
│   │
│   └───utilities
│       ├─── field_definitions.py
│       └─── processing_steps.py
|
├───reports
│   │
│   ├───input_profile
│   └───output_profile
│
└───tests
    ├───unittests
    │       ├───test_field_definitions.py
    │       └───test_processing_steps.py
    │
    ├───backtesting
    └───intergration testing
```

All of the action happens in the diabetes_code module. There are two files you should take note of:

- params.py
- create_publication.py

The file ``params.py`` contains all of the things that we expect to change from one publication
to the next. Indeed, if the methodology has not changed, then this should be the only file you need
to modify. This file specifies which publication we are creating, what data to use, etc.

The top-level script, ``create_publication.py`` is the highest level of abstraction and organises
the production of the publication data. This script imports functions and field definitions from
the sub-modules.

The ``processing_steps.py`` module holds all of the functions that do the complicated parts of the
data processing. It is these functions that get imported to the top level
``create_publication.py``. Some of the functions include:

```
- read_and_prepare_data()
- assign_record_scores()
- identify_best_record()
- create_golden_record_table()
- enrich_golden_record()
- produce_aggregates()
```

The ``field_definitions.py`` holds the rules to calculate all of the derived fields. By storing
the field definitions in this way, we can reuse them if needed but can also test them easily.

For example, here is how we define the 'CHOLESTEROL_NUMERATOR' field:

```
cholesterol_numerator = (
    F.when((F.col('AGE') >= 12) &
    (F.col("CHOLESTEROL_DATE").isNotNull()),
    1).otherwise(0)
)
```
# Testing
To run the set of unittests, backtesting and test-coverage we utilized the pytest and pytest-cov packages. To execute a unit test, simply type ```pytest``` and the script of your choice or ```pytest . ``` to run pytest on all tests.

```
pytest --cov=diabetes_code tests/ --cov-report term-missing
```

    Stmts - means the total lines of code you have in a specific file.
    Miss - total number of lines that are not covered by unittest.
    Cover - percentage of all line of code that are covered by unittest.
    Missing - lines of codes that are not covered.

Current coverage
```
Name                                              Stmts   Miss  Cover   Missing
-------------------------------------------------------------------------------
diabetes_code\__init__.py                             0      0   100%
diabetes_code\create_publication.py                  28     28     0%   1-76
diabetes_code\params.py                               1      0   100%
diabetes_code\utilities\__init__.py                   0      0   100%
diabetes_code\utilities\data_connections.py         106     75    29%   53-55, 68-87, 123-124, 134-152, 191-257
diabetes_code\utilities\field_definitions.py         23      0   100%
diabetes_code\utilities\populate_lite_tables.py      68     68     0%   1-572
diabetes_code\utilities\processing_steps.py          53      0   100%
diabetes_code\utilities\schema_definitions.py         0      0   100%
-------------------------------------------------------------------------------
TOTAL                                               279    171    39%
```
# Contributing
Currently, this project won't accept any pull requests. When this is permitted, relevant Contributing guidance will be released.
# Licence
National Diabetes Audit RAP codebase is released under the MIT License.

The documentation is © Crown copyright and available under the terms of the [Open Government 3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) licence.
