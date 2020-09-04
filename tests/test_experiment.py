from experiment import *
import pytest

"""For the shake of the testing process a workflow object is being created along with some task objects"""
w1 = Workflow(name="PTA", author="CMCC Foundation", abstract="Workflow for the analysis of  precipitationtrends "
                                                             "related to different scenarios. ${1} is ncores; ${2} "
                                                             "is the list of models (e.g. CMCC-CM|CMCC-CMS); ${3} "
                                                             "is the scenario (e.g. rcp45 or rcp85); ${4} is the "
                                                             "frequency (e.g. day or mon); ${5} is the percentile "
                                                             "(e.g. 0.9); ${6} is the past time subset "
                                                             "(e.g. 1976_2006); ${7} is the future time subset "
                                                             "(e.g. 2071_2101); ${8} is the geographic subset "
                                                             "(e.g. 30:45|0:40); ${9} is the grid of output map "
                                                             "using the format r<lon>x<lat> (e.g. r360x180), i.e. "
                                                             "a global regular lon/lat grid; ${10} import type "
                                                             "(optional), set to '1' in case only subsetting "
                                                             "data have to be imported (default); ${11} I/O server "
                                                             "type (optional).")
t1 = w1.newTask(operator='oph_createcontainer', arguments={'container': 'work', 'dim': 'lat|lon|time', 'dim_type': 'double|double|double', 'hierarchy': 'oph_base|oph_base|oph_time', 'base_time': '1976-01-01', 'calendar': 'standard', 'units': 'd'}, dependencies={})

w2 = Workflow(name="PTA_template", author="CMCC Foundation",
              abstract="Workflow for the analysis of precipitation trends related to different scenarios. ${1} is ncores; ${2} is the list of models (e.g. CMCC-CM|CMCC-CMS); ${3} is the scenario (e.g. rcp45 or rcp85); ${4} is the frequency (e.g. day or mon); ${5} is the percentile (e.g. 0.9); ${6} is the past time subset (e.g. 1976_2006); ${7} is the future time subset (e.g. 2071_2101); ${8} is the geographic subset (e.g. 30:45|0:40); ${9} is the grid of output map using the format r<lon>x<lat> (e.g. r360x180), i.e. a global regular lon/lat grid; ${10} import type (optional), set to '1' in case only subsetting data have to be imported (default); ${11} I/O server type (optional).")
t2 = w2.newTask(operator='oph_if')
t3 = Task(name="Create Historical Container", operator="oph_createcontainer", arguments={}, on_error="skip")


def test_deinit():
    w1.deinit()


@pytest.mark.parametrize(("task"),
                         [(t3), ("t3"), ([t3])])
def test_addTask(task):
    w1.addTask(task=task)



# the first will pass
@pytest.mark.parametrize(("taskname"),
                         [("sample_task1"), (5), ({4: 5})])
def test_get_task(taskname):
    task = w1.getTask(taskname=taskname)


# First four pass
@pytest.mark.parametrize(("filename"),
                         [("file1.json"), ("file1"), (".json"), ("[_6#$3]"), (""), (5), ({4: 5}), ([])])
def test_save(filename):
    w1.save(workflowname=filename)


# First and last pass
@pytest.mark.parametrize(("operator", "arguments", "dependencies", "name"),
                         [("oph_sub", {'container': 'work', 'dim': 'lat|lon|time', 'dim_type': 'double|double|double',
                                       'hierarchy': 'oph_base|oph_base|oph_time', 'base_time': '1976-01-01',
                                       'calendar': 'standard', 'units': 'd'}, {}, None),
                          ("oph_sub", {"sample1": 4, 3: "sample2"}, {}, 4),
                          ("oph_sub", {"sample1": 4, 3: "sample2"}, [], 3),
                          ("oph_sub", {"sample1": 4, "second_key": None}, {}, "sample_name")
                          ])
def test_newTask(operator, arguments, dependencies, name):
    w1.newTask(operator=operator, arguments=arguments, dependencies=dependencies, name=name)


# first must pass
@pytest.mark.parametrize(("workflow", "params", "dependency", "name"),
                         [(w2, {"$oph_importnc2_container_value": "historical"}, {}, "new_subworkflow2"),
                          (w2, {"$oph_importnc2_container_value": "historical"}, [], "new_subworkflow3"),
                          (w2, [], [], ""),
                          ("w2", {}, {}, "")])
def test_newSubWorkflow(workflow, params, dependency, name):
    w1.newSubWorkflow(workflow=workflow, params=params, dependency=dependency, name=name)


# only the last one will pass (you have to create a file named "file1.json")
@pytest.mark.parametrize(("file"),
                         [("file1"), (14), ({}), ("file1.json")])
def test_load(file):
    w1.load(file=file)


# only the first must pass
@pytest.mark.parametrize(("task", "argument"),
                         [(t2, "myargument"), (t2, ["myargument"]), (t2, {}), (t2, None), (t3, None)])
def test_addDependency(task, argument):
    t3.addDependency(task=task, argument=argument)