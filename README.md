ESDM PAV Python API and Client
==============================

*esdm-pav-client* is a [GPLv3](http://www.gnu.org/licenses/gpl-3.0.txt)-licensed Python package for modelling and executing a post-processing, analytics and visualisation (PAV) experiment to be executed with the ESDM PAV Runtime system.

It runs on Python 3.5, 3.6, 3.7, 3.8 and 3.9 is pure-Python code.

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

*Pip* package installer can also be used within the repo folder using the following in place of the last command:

``` {.sourceCode .bash}
pip install -e .
```

Examples
--------

#### Create a ESDM PAV experiment

Create a simple PAV experiment consisting of a single task (an Ophidia operator):

``` {.sourceCode .python}
from esdm_pav_client import Experiment

e1 = Experiment(name="Sample experiment", author="sample author",
              abstract='Example ESDM PAV workflow')
t1 = e1.newTask(name="Sample task", type="ophidia", operator="oph_list", 
              on_error="skip", arguments={"level": "2"})
```

#### Task experiments dependency management

Dependency can be specified to enforce an order in the execution of the tasks. Starting from the previous example, a dependent task is added (e.g., an Ophidia operator):

``` {.sourceCode .python}
t2 = e1.newTask(name="Sample task 2", type="ophidia", operator='oph_createcontainer', 
                arguments={'container': "test", 'dim': 'lat|lon|time'},
                dependencies={t1: None}) 
```

#### Dynamic replacement of argument values in tasks

Arguments value can be dynamically replaced in a PAV experiment upon submission time. Considering the previous example, the container argument value can be made dynamic:

``` {.sourceCode .python}
t2 = e1.newTask(name="Sample task 2", type="ophidia", operator='oph_createcontainer', 
                arguments={'container': "$1", 'dim': 'lat|lon|time'},
                dependencies={t1: None})
```

#### Save a ESDM PAV experiment document

Save the ESDM PAV experiment as JSON document

``` {.sourceCode .python}
e1.save("example.json")
```

#### Validate a ESDM PAV experiment document

Validate the ESDM PAV experiment document before the submission

``` {.sourceCode .python}
e1.check()
```

#### Submit a ESDM PAV experiment for execution

Submit the experiment created for execution on the ESDM PAV runtime

``` {.sourceCode .python}
from esdm_pav_client import Workflow, Experiment
w1 = Workflow(e1)
w1.submit("2")
```

#### Monitor a running ESDM PAV experiment

Monitor the running experiment on the ESDM PAV runtime. The visual mode argument shows a graphical view of the experiment execution status

``` {.sourceCode .python}
w1.monitor(visual_mode=True)
```

#### Cancel a ESDM PAV experiment

Cancel the experiment on the ESDM PAV runtime.

``` {.sourceCode .python}
w1.cancel()
```

#### Load a ESDM PAV experiment document

Load a PAV experiment from the JSON document

``` {.sourceCode .python}
e1 = Experiment.load("example.json")
```

#### Additional information on the methods

Docstrings are available for all the Workflow, Experiment and Task classes. To get additional information run:

``` {.sourceCode .python}
from esdm_pav_client import Workflow, Experiment, Task
help(Workflow)
help(Experiment)
help(Task)
```

#### Run a ESDM PAV experiment with the CLI

To submit the execution of a PAV experiment document on the ESDM PAV runtime:

``` {.sourceCode .bash}
$prefix/esdm-pav-client -w example.json 2
```

To monitor the execution of a running PAV experiment document on the ESDM PAV runtime:

``` {.sourceCode .bash}
$prefix/esdm-pav-client -w example.json 2 -m
```

To cancel a running PAV experiment document on the ESDM PAV runtime:

``` {.sourceCode .bash}
$prefix/esdm-pav-client -c <workflow_id>
```

A full experiment example
-------------------------

The following code show a full PAV experiment composed of CDO tasks, the commands to save the related JSON file and for its submission

``` {.sourceCode .python}
from esdm_pav_client import Workflow, Experiment, Task
 
e1 = Experiment(name="CDO PAV experiment example",
          author="ESiWACE2",
          abstract="Example of PAV experiment with CDO")
t1 = e1.newTask(name="Regrid",
          type="cdo",
          operator='-remapbil,r90x45',
          arguments={'input': 'path/to/infile.nc', 
                'output': 'esdm://out_container'})
t2 = e1.newTask(name="Max",
          type="cdo",
          operator='-timmax',
          arguments={'output': 'esdm://container_max'},
          dependencies={t1:'input'})
t3 = e1.newTask(name="Min",
          type="cdo",
          operator='-timmin',
          arguments={'output': 'esdm://container_min'},
          dependencies={t1:'input'})
t4 = e1.newTask(name="Avg",
          type="cdo",
          operator='-timavg',
          arguments={'output': 'esdm://container_avg'},
          dependencies={t1:'input'})

e1.save("example.json")
e1.check()

w1 = Workflow(e1)
w1.submit()
```

Additional examples can be found under the `examples` folder.

Acknowledgement
---------------

This software has been developed in the context of the *[ESiWACE2](http://www.esiwace.eu)* project: the *Centre of Excellence in Simulation of Weather and Climate in Europe phase 2*. ESiWACE2 has received funding from the European Union’s Horizon 2020 research and innovation programme under grant agreement No. 823988.
