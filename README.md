ESDM PAV Python API and Client
==============================

*esdm-pav-client* is a [GPLv3](http://www.gnu.org/licenses/gpl-3.0.txt)-licensed Python package for modelling and executing a post-processing, analytics and visualisation (PAV) experiment to be executed with the ESDM PAV Runtime system.

It runs on Python 3.5, 3.6 and 3.7 is pure-Python code.

It provides 2 main modules:

-   An API for creating and submitting PAV experiments: *Workflow* and *Task* Python classes;
-   A CLI for PAV documents submission: a Python-based client called *esdm-pav-client*.

Dependencies
------------

-   [PyOphidia](https://github.com/OphidiaBigData/PyOphidia): the Ophidia Python bindings
-   [click](https://click.palletsprojects.com): a Python package for creating beautiful command line interfaces in a composable way
-   [graphviz](https://graphviz.readthedocs.io/en/stable/): Python interface to facilitates the creation and rendering of graph descriptions in the DOT language of Graphviz

Installation from sources
-------------------------

To install the package run the following commands:

``` {.sourceCode .bash}
git clone https://github.com/OphidiaBigData/esdm-pav-client
cd esdm-pav-client
python setup.py install
```

Examples
--------

#### Create a ESDM PAV experiment

Create a simple PAV experiment consisting of a single task (an Ophidia operator):

``` {.sourceCode .python}
from esdm_pav_client import Workflow

w1 = Workflow(name='Example experiment', author='Author', 
              abstract='Example ESDM PAV workflow')
t1 = w1.newTask(name="List cubes", type="ophidia",
                operator='oph_list', arguments={'level': '2'})
```

#### Task experiments dependency management

Dependency can be specified to enforce an order in the execution of the tasks. Starting from the previous example, a dependent task is added (e.g., an Ophidia operator):

``` {.sourceCode .python}
t2 = w1.newTask(name="Import", type="ophidia",
                operator='oph_createcontainer', 
                arguments={'container': "test", 'dim': 'lat|lon|time'},
                dependencies={t1: None})
```

#### Dynamic replacement of argument values in tasks

Arguments value can be dynamically replaced in a PAV experiment upon submission time. Considering the previous example, the container argument value can be made dynamic:

``` {.sourceCode .python}
t2 = w1.newTask(name="Import", type="ophidia",
                operator='oph_createcontainer', 
                arguments={'container': "$1", 'dim': 'lat|lon|time'},
                dependencies={t1: None})
```

#### Save a ESDM PAV experiment document

Save the ESDM PAV experiment as JSON document

``` {.sourceCode .python}
w1.save("example.json")
```

#### Submit a ESDM PAV experiment for execution

Submit the experiment created for execution on the ESDM PAV runtime

``` {.sourceCode .python}
w1.submit("2")
```

#### Load a ESDM PAV experiment document

Load a PAV experiment from the JSON document

``` {.sourceCode .python}
w1 = Workflow.load("example.json")
```

#### Additional information on the methods

Docstrings are available for both Workflow and Task classes. To get additional information run:

``` {.sourceCode .python}
from esdm_pav_client import Workflow, Task
help(Workflow)
help(Task)
```

#### Run a ESDM PAV experiment with the CLI

To submit the execution of a PAV experiment document on the ESDM PAV runtime:

``` {.sourceCode .bash}
$prefix/esdm-pav-client example.json 2
```

Acknowledgement
---------------

This software has been developed in the context of the *[ESiWACE2](http://www.esiwace.eu)* project: the *Centre of Excellence in Simulation of Weather and Climate in Europe phase 2*. ESiWACE2 has received funding from the European Union’s Horizon 2020 research and innovation programme under grant agreement No. 823988.
