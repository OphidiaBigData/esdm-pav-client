

from esdm_pav_client import Task, Workflow
import pytest

"""A workflow object is being created along with some task objects for the
   testing process"""
w1 = Workflow(
    name="Sample_Workflow",
    author="Author_name",
    abstract="Example workflow for testing",
)
t2 = w1.newTask(name="mytask1", operator='oph_script',
                arguments={'script': '/gfs-data/home/evachlas/sleep.sh'}, dependencies={})
t3 = w1.newTask(name="mytask2", operator='oph_script',
                arguments={'script': '/gfs-data/home/evachlas/sleep.sh'}, dependencies={t2: None})
w1.submit()

w2 = Workflow(
    name="Sample_Workflow_template",
    author="Author_name",
    abstract="Second example workflow for testing",
)
t2 = w2.newTask(operator="oph_if")
t3 = Task(
    name="Create Container",
    operator="oph_createcontainer",
    arguments={},
)

def test_deinit():
    w1.deinit()

@pytest.mark.parametrize(("task"), [(t3), ("t3"), ([t3])])
def test_addTask(task):
    w1.addTask(task=task)


# the first will pass
@pytest.mark.parametrize(("taskname"), [("sample_task1"), (5), ({4: 5})])
def test_get_task(taskname):
    w1.getTask(taskname=taskname)


# First four pass
@pytest.mark.parametrize(
    ("filename"),
    [
        ("file1.json"),
        ("file1"),
        (".json"),
        ("[_6#$3]"),
        (""),
        (5),
        ({4: 5}),
        ([]),
    ],
)
def test_save(filename):
    w1.save(workflowname=filename)


# First and last pass
@pytest.mark.parametrize(
    ("operator", "arguments", "dependencies", "name"),
    [
        (
            "oph_sub",
            {
                "container": "work",
                "dim": "lat|lon|time",
                "dim_type": "double|double|double",
            },
            {},
            None,
        ),
        ("oph_sub", {"sample1": 4, 3: "sample2"}, {}, 4),
        ("oph_sub", {"sample1": 4, 3: "sample2"}, [], 3),
        ("oph_sub", {"sample1": 4, "second_key": None}, {}, "sample_name"),
    ],
)
def test_newTask(operator, arguments, dependencies, name):
    w1.newTask(
        operator=operator,
        arguments=arguments,
        dependencies=dependencies,
        name=name,
    )


# first must pass
@pytest.mark.parametrize(
    ("workflow", "params", "dependency", "name"),
    [
        (
            w2,
            {"$oph_importnc2_container_val": "historical"},
            {},
            "new_subworkflow2",
        ),
        (
            w2,
            {"$oph_importnc2_container_val": "historical"},
            [],
            "new_subworkflow3",
        ),
        (w2, [], [], ""),
        ("w2", {}, {}, ""),
    ],
)
def test_newSubWorkflow(workflow, params, dependency, name):
    w1.newSubWorkflow(
        workflow=workflow, params=params, dependency=dependency, name=name
    )


# only the last one will pass (you have to create a file named "file1.json")
@pytest.mark.parametrize(("file"), [("file1"), (14), ({}), ("file1.json")])
def test_load(file):
    w1.load(file=file)


# only the first must pass
@pytest.mark.parametrize(
    ("task", "argument"),
    [
        (t2, "myargument"),
        (t2, ["myargument"]),
        (t2, {}),
        (t2, None),
        (t3, None),
    ],
)
def test_addDependency(task, argument):
    t3.addDependency(task=task, argument=argument)

@pytest.mark.parametrize(
    ("server", "port"),
    [
        ("sample_server", "sample_port"),
        (None, None),
        (1, 2),
        ("sample_use", None),
        ([], 0),
    ],
)
def test_submit(server, port):
    w1.submit(server=server, port=port)

# First three should pass
@pytest.mark.parametrize(
    ("filename", "visual"),
    [
        ("sample.dot", True),
        ("sample.sample", False),
        (".pdf", True),
        (15, False),
        (15, "False"),
        ("sample.pdf", "True")
    ]
)
def test_check(filename, visual):
    w1.check(filename=filename, visual=visual)


def test_cancel():
    w1.cancel()

# First five should pass
@pytest.mark.parametrize(
    ("frequency", "iterative", "visual_mode"),
    [
        (10, True, False),
        (10, True, True),
        (10, False, True),
        (10, False, False),
        (100, True, False),
        (10, True, False),
        ("10", True, False),
        (10, "True", False),
        (10, True, "False"),
        (None, True, False),

    ]
)
def test_monitor(frequency, iterative, visual_mode):
    w1.monitor(frequency=frequency, iterative=iterative, visual_mode=visual_mode)