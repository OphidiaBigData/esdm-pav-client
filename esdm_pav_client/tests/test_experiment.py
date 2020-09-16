
from esdm_pav_client import *
import pytest

"""For the shake of the testing process a workflow object is being created along with some task objects"""
w1 = Workflow(name="Sample_Workflow", author="CMCC Foundation", abstract="Example workflow for testing")
t1 = w1.newTask(operator='oph_createcontainer', arguments={'container': 'work', 'dim': 'lat|lon|time',
                                                           'dim_type': 'double|double|double',
                                                           'hierarchy': 'oph_base|oph_base|oph_time'}, dependencies={})

w2 = Workflow(name="Sample_Workflow_template", author="CMCC Foundation",
              abstract="Second example workflow for testing")
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
                                       'hierarchy': 'oph_base|oph_base|oph_time'}, {}, None),
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


@pytest.mark.parametrize(("username", "server", "port", "password"),
                         [("sample_user", "sample_server", "sample_port", "sample_password"),
                          (None, None, None, None),
                          (1, 2, 3, 4),
                          ("sample_use", 2, 3, None),
                          ([], {}, [], 0)])
def test_submit(username, server, port, password):
    w1.submit(username=username, server=server, port=port, password=password)