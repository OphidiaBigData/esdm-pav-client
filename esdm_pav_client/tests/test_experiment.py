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
t4 = e2.newTask(operator="oph_if")
t5 = Task(
    name="Create Container",
    operator="oph_createcontainer",
    arguments={},
)


def test_deinit():
    e1.deinit()


@pytest.mark.parametrize(("task"), [(t3), ("t3"), ([t3])])
def test_addTask(task):
    e1.addTask(task=task)


# the first will pass
@pytest.mark.parametrize(("taskname"), [("sample_task1"), (5), ({4: 5})])
def test_get_task(taskname):
    e1.getTask(taskname=taskname)


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
    e1.save(experimentname=filename)


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
    e1.newTask(
        operator=operator,
        arguments=arguments,
        dependencies=dependencies,
        name=name,
    )


# first must pass
@pytest.mark.parametrize(
    ("workflow", "params", "dependency"),
    [
        (
            e2,
            {"$oph_importnc2_container_val": "historical"},
            {t4: None}
        ),
        (
            e2,
            {"$oph_importnc2_container_val": "historical"},
            {}
        ),
        (e2, [], {}),
        ("w2", {}, {}),
    ],
)
def test_newSubExperiment(workflow, params, dependency):
    e2.newSubexperiment(
        experiment=workflow, params=params, dependency=dependency
    )


# only the last one will pass (you have to create a file named "file1.json")
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
