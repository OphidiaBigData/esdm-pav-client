from esdm_pav_client import Task, Workflow, Experiment
import pytest

"""An experiment object is being created along with some task objects for the
   testing process"""
e1 = Experiment(
    name="Sample_Workflow",
    author="Author_name",
    abstract="Example workflow for testing",
)
t2 = e1.newTask(
    name="mytask1",
    operator="oph_script",
    arguments={"script": "sleep.sh"},
    dependencies={},
)
t3 = e1.newTask(
    name="mytask2",
    operator="oph_script",
    arguments={"script": "sleep.sh"},
    dependencies={t2: None},
)

e2 = Experiment(
    name="Sample_Workflow_template",
    author="Author_name",
    abstract="Second example workflow for testing",
)
t2 = e2.newTask(operator="oph_if")
t3 = Task(
    name="Create Container",
    operator="oph_createcontainer",
    arguments={},
)


def test_deinit():
    e1.deinit()

@pytest.mark.skip(reason="no way of currently testing this")
@pytest.mark.parametrize(("task"), [(t3), ("t3"), ([t3])])
def test_addTask(task):
    e1.addTask(task=task)


# the first will pass
@pytest.mark.skip(reason="no way of currently testing this")
@pytest.mark.parametrize(("taskname"), [("sample_task1"), (5), ({4: 5})])
def test_get_task(taskname):
    e1.getTask(taskname=taskname)


# First four pass
@pytest.mark.skip(reason="no way of currently testing this")
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
    e1.save(experimentname=filename)


# First and last pass
@pytest.mark.skip(reason="no way of currently testing this")
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
    e1.newTask(
        operator=operator,
        arguments=arguments,
        dependencies=dependencies,
        name=name,
    )


# first must pass
@pytest.mark.skip(reason="no way of currently testing this")
@pytest.mark.parametrize(
    ("workflow", "params", "dependency", "name"),
    [
        (
            e2,
            {"$oph_importnc2_container_val": "historical"},
            {},
            "new_subworkflow2",
        ),
        (
            e2,
            {"$oph_importnc2_container_val": "historical"},
            [],
            "new_subworkflow3",
        ),
        (e2, [], [], ""),
        ("w2", {}, {}, ""),
    ],
)
def test_newSubExperiment(workflow, params, dependency, name):
    e2.newSubexperiment(
        experiment=workflow, params=params, dependencies=dependency, name=name
    )


# only the last one will pass (you have to create a file named "file1.json")
@pytest.mark.skip(reason="no way of currently testing this")
@pytest.mark.parametrize(("file"), [("file1"), (14), ({}), ("file1.json")])
def test_load(file):
    e2.load(file=file)


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


# First three should pass
@pytest.mark.skip(reason="no way of currently testing this")
@pytest.mark.parametrize(
    ("filename", "visual"),
    [
        ("sample.dot", True),
        ("sample.sample", False),
        (".pdf", True),
        (15, False),
        (15, "False"),
        ("sample.pdf", "True"),
    ],
)
def test_check(filename, visual):
    e1.check(filename=filename, visual=visual)

# First two should pass
@pytest.mark.parametrize(
    ("name", "index_values", "parallel", "iteration_branch", "dependency"),
    [
        ("sample_name", "$1", "yes", e2, None),
        ("sample_name", "$1", "yes", e2, {t3: None}),
        ("sample_name", "$1", "yes", e2, "t3"),
        ("sample_name", "$1", "yes", "e2", None),
        ("sample_name", 1, True, e2, None),
        (None, "$1", "yes", e2, None),
        (t3, "$1", None, e2, t3),
        ("sample_name", "$1", None, e2, None),
        ("sample_name", "$1", "yes", None, None),
        ("sample_name", "$1", "yes", [t3], None),
        ("sample_name", "$1", "yes", False, None),
        ("sample_name", "$1", False, e2, None),
        ("sample_name", "$1", "yes", e2, False),
        (None, None, None, None, None),

    ]
)
def test_newForTask(name, index_values, parallel, iteration_branch,
                    dependency):
    e1.newForTask(name=name, index_values=index_values, parallel=parallel,
               iteration_branch=iteration_branch, dependency=dependency)

# The first one should pass
@pytest.mark.parametrize(
    ("condition", "if_branch", "else_branch", "dependency"),
    [
        ("$1", e2, e2, {t2: None}),
        ("$1", e2, "e2", None),
        ("$1", e2, None, t2),
        (1, e2, e2, None),
        ("$1", None, e2, None),
        ("$1", e2, e2, "None"),
        ("$1", e2, False, False),
        ("$1", "e2", e2, None),
        (None, e2, e2, None),
    ]
)
def test_newIfTask(condition, if_branch, else_branch, dependency):
    e1.newIfTask(condition=condition, if_branch=if_branch,
                 else_branch=else_branch, dependency=dependency)