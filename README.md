# vsf
[![Build Status](https://travis-ci.com/she-dares/VSF.svg?token=nXkzxgcfECHNJCursNs3&branch=master)](https://travis-ci.com/she-dares/VSF)
[![Maintainability](https://api.codeclimate.com/v1/badges/91d03878cbf2dfb6a899/maintainability)](https://codeclimate.com/github/she-dares/VSF/maintainability)
[![Test Coverage](https://api.codeclimate.com/v1/badges/91d03878cbf2dfb6a899/test_coverage)](https://codeclimate.com/github/she-dares/VSF/test_coverage)

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Name Of Project
Smart Family Web Analytics Portal System

## Project Overview
Solve a problem for work. VSF app is a product from our company for families to track location, content filtering and parental controls for the young ones.

## Objective
We are proposing to derive value from the mobile analytics data collected from VSF App to enhance customer satisfaction and improve product performance through targeted marketing campaigns.

## Initial Setup

### `cookiecutter`
Used the cookiecutter template to render a new app and linked to our repo

### `cookiecutter-django`
[cookiecutter-django](https://github.com/pydanny/cookiecutter-django) used this to
consult and use as a supplement to build our django application

Used the following settings to render the django template:

| setting | value |
| - | - |
| Project name | `vsf` |
| timezone | `UTC` |
| use_docker | `y` |
| use_travisci | `y` |
| debug | `y` |

### `utils`
* Created a utils package for dask and luigi utilities, to read and write files from CSVTarget/ParquetTarget

### `vsf`
DataPipeline tasks- `datapipeline.py`, `db_con.py`
* These tasks were implemented to be able to take the gzip'ed txt file for processing and 
clean it and split the EventProperties column to 2 column Event and Properties and 
also to be able to successfully load into a sqlite database.
* We will be leveraging this code, later on in our production system, when we need to run this to be able to load 
the data to a 'Hive' database.

As part of this exercise and to test the Proof Of Concept, we have used sqlite database

Luigi tasks for workflow - `tasks.py`
* Created a Luigi External task `VerfyFileArrived` to verify that the file for the processing date has arrived and available for processing.
* Created a Luigi External task `ArchiveGzFile` to take the backup up of .gz file and moved it to the archive directory at the Local Server. 
* Created a Luigi task to `CleanandProcessData` to do the cleaning/processing of the files and copy the files 
(POC - from S3 to local directory file://data/amplitude/ in parquet format)
(Production- from Local to HDFS directory) 
* Created a Luigi task `ByMdn` to save only pertinent fields needed for the activity event processing (file://data/amplitude/by_mdn/)
* Luigi tasks to load data from other sources so that we can create the necessary analytics for processing - `djangotasks.py`
* Created a Luigi External task `LineDimVer` to verify that the target exists for the processing to load the Line Dimension data.
* Created a Luigi task `LineDimLoad` - this task outputs a local ParquetTarget in ./data/linedim/ for further load into the database.
* Created a Luigi External task `AccountDimVer` to verify that the target exists for the processing to load the Account Dimension data.
* Created a Luigi task `AccountDimLoad` - this task outputs a local ParquetTarget in ./data/accountdim/ for further load into the database.
* Created a Luigi External task `LimitFactVer` to verify that the target exists for the processing. This task loads the Limit Facts data.
* Created a Luigi task `LimitFactLoad` - this task outputs a local ParquetTarget in ./data/limitfact/ for further load into the database.

### `vsf_eventapp` - vsf_eventapp/models.py, serializers.py, views.py
* Created a django application vsf_eventapp
* Generated the ORM models, views and serializers 
* Built APIs and the authentication mechanisms
* Built queries to aggregate data for the data analytics and used the Django REST framework for UI

#### Ensuring a Star Schema
All our work of data modelling exemplifies a Star Schema. 
The data models were created adhering to the database design principles of a
[star-schema](https://www.vertabelo.com/blog/technical-articles/data-warehouse-modeling-star-schema-vs-snowflake-schema).

Migration files in vsf_eventapp/migrations

#### Loading of the data - vsf_appdataprep.py in vsf_eventapp/management/commands
Our load was all  atomic, by making use of django batteries copiously-
  * [transactions.atomic](https://docs.djangoproject.com/en/2.2/topics/db/transactions/#django.db.transaction.atomic)
  * [queryset.bulk_create](https://docs.djangoproject.com/en/2.1/ref/models/querysets/#bulk-create)
  * [queryset.in_bulk](https://docs.djangoproject.com/en/2.1/ref/models/querysets/#in-bulk)

#### Admin Pages
Admin models were created so that everything can be viewed in the admin UI as well!

#### URLs
All the urls were respectively added, to be able to browse the api via the CoreAPI UI
[/docs/](http://localhost:8000/docs/) as well as the direct
[/vsf/api](http://localhost:8000/vsf/api/)!

#### Travis Answers

Updated `.travis.yml` `answers` build:

```yaml
- stage: answers
  script:
    - python manage.py migrate
    - python manage.py vsf_appdataprep
   
```
### Pipenv
Used the following packages for the django environment and other packages as needed
```bash
pipenv install django django-environ djangorestframework django-model-utils argon2-cffi django-allauth django-crispy-forms django-extensions coreapi
pipenv install --dev pytest-django django-debug-toolbar pytest-cov factory-boy
```

## Testing 

Wrotes tests against endpoints using [DRF
Testing](https://www.django-rest-framework.org/api-guide/testing/).

For Django tests, used Django's TestCase subclass (`django.test.TestCase`) when
you need to test against a database.  See [testing
docs](https://docs.djangoproject.com/en/2.2/topics/testing/).  

Used vanilla `TestCase`'s where a DB or a live server was not needed.

```bash

----------- coverage: platform win32, python 3.7.0-final-0 -----------
Name                                                            Stmts   Miss Branch BrPart  Cover
-------------------------------------------------------------------------------------------------
utils\__init__.py                                                   0      0      0      0   100%
utils\luigi\__init__.py                                             0      0      0      0   100%
utils\luigi\dask\__init__.py                                        0      0      0      0   100%
utils\luigi\dask\target.py                                        104     22     28     11    75%
utils\luigi\task.py                                                29      0     12      0   100%
vsf\__init__.py                                                     2      0      2      0   100%
vsf\__main__.py                                                     0      0      0      0   100%
vsf\conftest.py                                                    12      0      0      0   100%
vsf\contrib\__init__.py                                             0      0      0      0   100%
vsf\contrib\sites\__init__.py                                       0      0      0      0   100%
vsf\contrib\sites\migrations\0001_initial.py                        6      0      0      0   100%
vsf\contrib\sites\migrations\0002_alter_domain_unique.py            5      0      0      0   100%
vsf\contrib\sites\migrations\0003_set_site_domain_and_name.py      11      2      0      0    82%
vsf\contrib\sites\migrations\__init__.py                            0      0      0      0   100%
vsf\djangotasks.py                                                 56     15      6      0    66%
vsf\tasks.py                                                       56      6      2      1    88%
vsf\users\__init__.py                                               0      0      0      0   100%
vsf\users\admin.py                                                 12      0      2      0   100%
vsf\users\apps.py                                                  10      0      0      0   100%
vsf\users\forms.py                                                 18      0      0      0   100%
vsf\users\migrations\0001_initial.py                                8      0      0      0   100%
vsf\users\migrations\__init__.py                                    0      0      0      0   100%
vsf\users\models.py                                                 8      0      0      0   100%
vsf\users\tests\__init__.py                                         0      0      0      0   100%
vsf\users\tests\factories.py                                       14      0      0      0   100%
vsf\users\urls.py                                                   4      0      0      0   100%
vsf\users\views.py                                                 28      2      0      0    93%
vsf\utils\__init__.py                                               0      0      0      0   100%
vsf_eventapp\__init__.py                                            0      0      0      0   100%
vsf_eventapp\admin.py                                               7      0      0      0   100%
vsf_eventapp\apps.py                                                3      0      0      0   100%
vsf_eventapp\management\__init__.py                                 0      0      0      0   100%
vsf_eventapp\management\commands\__init__.py                        0      0      0      0   100%
vsf_eventapp\models.py                                             22      0      0      0   100%
vsf_eventapp\serializers.py                                        11      0      0      0   100%
vsf_eventapp\urls.py                                               10      0      0      0   100%
vsf_eventapp\views.py                                              22      7      0      0    68%
-------------------------------------------------------------------------------------------------
TOTAL                                                             458     54     52     12    86%

======================================================================================== 14 passed, 6 warnings in 10.81s ========================================================================================

```

## Credits and Acknowledgements
Lakshmi Umarale, Rahul Sharma, Sheetal Darekar

## Resources
- [Django Documentation](https://docs.djangoproject.com/en/2.2/)
- [Django Rest Framework](https://www.django-rest-framework.org/)
- [Luigi Tasks](https://luigi.readthedocs.io/en/stable/tasks.html/)
- [Dask Tasks](https://docs.dask.org/en/latest/)