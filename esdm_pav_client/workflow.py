

class Workflow:
    """
    Creates or loads a ESDM PAV experiment workflow. It also contains methods
    to manipulate the workflow.

    A workflow is a sequence of tasks. Each task can be either independent or
    dependent on other tasks, for instance it processes the output of other
    tasks.

    Construction::
    w1 = Workflow(name="sample", author="sample author",
                    abstract="sample abstract", on_error=None, run=None,
                    ncores=1, nthreads=None)


    Parameters
    ----------
    name: str
        ESDM PAV experiment name
    author: str, optional
        ESDM PAV experiment author
    abstract: str, optional
        ESDM PAV experiment description
    on_error: str, optional
        behaviour in case of error
    run: str, optional
        enable actual execution, yes or no
    nthreads: str, optional
        number of threads
    ncores: int, optional
        number of cores

    """

    attributes = [
        "exec_mode",
        "on_error",
        "run",
        "nthreads",
        "ncores",
    ]
    active_attributes = ["name", "author", "abstract"]
    task_attributes = ["run", "on_error", "type"]
    task_name_counter = 1
    subworkflow_names = []

    def __init__(self, name, author=None, abstract=None, **kwargs):
        for k in kwargs.keys():
            if k not in self.attributes:
                raise AttributeError("Unknown workflow argument: {0}".format(k))
            self.active_attributes.append(k)
        self.name = name
        self.author = author
        self.abstract = abstract
        self.exec_mode = "sync"
        self.tasks = []
        self.__dict__.update(kwargs)

    def deinit(self):
        """
        Reverse the initialization of the object
        """
        for k in self.active_attributes:
            self.__delattr__(k)

    def addTask(self, task):
        """
        Adds a task to the ESDM PAV experiment workflow

        Parameters
        ----------
        task : <class 'esdm_pav_client.task.Task'>
            Task to be added to the workflow

        Raises
        ------
        AttributeError
            If the task name is already in the workflow or if a dependency is
            not fulfilled

        Example
        -------
        t1 = Task(name="sample task", operator='oph_reduce',
                    arguments={'operation': 'avg'})
        w1.addTask(t1)
        """
        if "name" not in task.__dict__.keys() or task.name is None:
            task.name = self.name + "_{0}".format(self.task_name_counter)
        if task.__dict__["name"] in [t.__dict__["name"] for t in self.tasks]:
            raise AttributeError("task already exists")
        if task.__dict__["dependencies"]:
            for dependency in task.__dict__["dependencies"]:
                if dependency["task"] not in [
                    task.__dict__["name"] for task in self.tasks
                ]:
                    raise AttributeError("dependency not fulfilled")
        self.task_name_counter += 1
        self.tasks.append(task)

    def getTask(self, taskname):
        """
        Retrieve from the ESDM PAV workflow the esdm_pav_client.task.Task
        object with the given task name

        Parameters
        ----------
        taskname : str
            The name of the task to be found in the workflow

        Returns
        -------
        task : <class 'esdm_pav_client.task.Task'>
            Returns the first task found
        None : Nonetype
            If no task was found then returns None

        Example
        -------
        t1 = Task(name="task_one", operator="oph_reduce",
                    arguments={'operation': 'avg'})
        task = w1.getTask(taskname="task_one")
        """
        tasks = [t for t in self.tasks if t["name"] == taskname]
        if len(tasks) == 1:
            return tasks[0]
        elif len(tasks) == 0:
            return None

    def save(self, workflowname):
        """
        Save the ESDM PAV experiment workflow as a JSON document

        Parameters
        ----------
        workflowname : str
            The path to the ESDM PAV document file where the workflow is being
            saved

        Example
        -------
        from esdm_pav_client import Workflow
        w1 = Workflow(name="sample name", author="sample author",
                        abstract="sample abstract")
        w1.save("sample_workflow")

        Raises
        ------
        AttributeError
            If worfklowname is not a string or it is empty
        """
        import json
        import os

        if not isinstance(workflowname, str):
            raise AttributeError("workflowname must be string")
        if len(workflowname) == 0:
            raise AttributeError(
                "workflowname must contain more than 1 characters"
            )
        data = dict(self.__dict__)
        if "task_name_counter" in data.keys():
            del data["task_name_counter"]
        if not workflowname.endswith(".json"):
            workflowname += ".json"
        data["tasks"] = [t.__dict__ for t in self.tasks]
        with open(os.path.join(os.getcwd(), workflowname), "w") as fp:
            json.dump(data, fp, indent=4)

    def newTask(
        self, operator, arguments={}, dependencies={}, name=None, **kwargs
    ):
        """
        Adds a new Task in the ESDM PAV experiment workflow without the need
        of creating a esdm_pav_client.task.Task object

        Attributes
        ----------
        operator : str
            operator name
        arguments : dict, optional
            dict of user-defined operator arguments as key=value pairs
        dependencies : dict, optional
            a dict of dependencies for the task
        name : str, optional
            the name of the task
        type : str, optional
            type of the task
        on_error : str, optional
            behaviour in case of error
        run : str, optional
            enable actual execution, yes or no

        Returns
        -------
        t : <class 'esdm_pav_client.task.Task'>
            Returns the task that was created and added to the workflow

        Raises
        ------
        AttributeError
            Raises an AttributeError if the given arguments are not of the
            proper type or are not defined by the schema

        Example
        -------
        w1 = Workflow(name="Experiment 1", author="sample author",
                        abstract="sample abstract")
        t1 = w1.newTask(operator="oph_reduce", arguments={'operation': 'avg'},
                          dependencies={})
        """
        from .task import Task

        def parameter_check(operator, arguments, dependencies, name):
            if not isinstance(operator, str):
                raise AttributeError("operator must be a string")
            if not isinstance(arguments, dict):
                raise AttributeError("arguments must be a dict")
            if not isinstance(dependencies, dict):
                raise AttributeError("dependencies must be a dict")
            if not isinstance(name, str) and name is not None:
                raise AttributeError("name must be a string")

        parameter_check(operator, arguments, dependencies, name)
        t = Task(operator=operator, arguments=arguments, name=name)
        if dependencies:
            for k in dependencies.keys():
                if dependencies[k]:
                    t.addDependency(task=k, argument=dependencies[k])
                else:
                    t.addDependency(task=k)
        for k in kwargs.keys():
            if k not in self.task_attributes:
                raise AttributeError("Unknown Task argument: {0}".format(k))
        t.__dict__.update(kwargs)
        self.addTask(t)
        return t

    def newSubWorkflow(self, workflow, params, dependencies={}, name=None):
        """
        Embeds an ESDM PAV experiment workflow into another workflow

        Parameters
        ----------
        workflow : <class 'esdm_pav_client.workflow.Workflow'>
            The workflow that will be embeded into our main workflow
        params : dict of keywords
            a dict of keywords that will be used to replace placeholders in
            the tasks
        dependencies : list, optional
            list of dependencies
        name : str, optional
            a unique name for the workflow's tasks

        Returns
        -------
        A list of the leaf tasks of the subworkflow

        Raises
        ------
        AttributeError
            Raises AttributeError when there's an error with the workflows
            (same name or non-existent), or when the dependencies are not
            fulfilled

        Example
        -------
        w1 = Workflow(name="Experiment 1", author="sample author 1",
                        abstract="sample abstract 1")
        w2 = Workflow(name="Experiment 2", author="sample author 2",
                        abstract="sample abstract 2")
        t1 = w2.newTask(operator='oph_reduce', arguments={'operation': 'avg'})
        task_array = w1.newSubWorkflow(name="new_subworkflow", workflow=w2,
                                         params={}, dependencies=[])
        """

        from .task import Task

        def parameter_check(params, dependencies, name):
            if not isinstance(params, dict):
                raise AttributeError("params must be dict")
            if not isinstance(dependencies, dict):
                raise AttributeError("dependencies must be dict")
            if not isinstance(name, str):
                raise AttributeError("name must be string")

        def validate_workflow(w1, w2):
            if not isinstance(w2, Workflow) or w1.name == w2.name:
                raise AttributeError("Wrong workflow or same workflows")

        def dependency_check(dependency):
            if not isinstance(dependency, dict):
                raise AttributeError("dependency must be a list")
            if len(dependency.keys()) > 2:
                raise AttributeError("Wrong dependency arguments")
            elif len(dependency.keys()) == 2:
                if ("task" not in dependency.keys()) or (
                    "argument" not in dependency.keys()
                ):
                    raise AttributeError("Wrong dependency arguments")
            else:
                if "task" not in dependency.keys():
                    raise AttributeError("Wrong dependency arguments")

        def find_root_tasks_add_dependencies(task, dependencies, new_task):
            if len(task.dependencies) == 0:
                for dependency in dependencies:
                    dependency_check(dependency)
                    if len(dependency.keys()) == 1:
                        new_task.addDependency(task=dependency["task"])
                    else:
                        new_task.addDependency(
                            task=dependency["task"],
                            argument=dependency["argument"],
                        )

        def add_dependencies(task, new_task, prefix):
            if len(task.dependencies) > 0:
                for d in task.dependencies:
                    dep_dict = dict(d)
                    dep_dict["task"] = fix_dependency_name(d["task"], prefix)
                    new_task.copyDependency(dep_dict)

        def fix_dependency_name(dependency_name, prefix):
            return "{0}_{1}".format(prefix, dependency_name)

        def add_task_name(given_name, previous_name, task_id):
            if given_name:
                return "{0}_{1}".format(given_name, previous_name)
            else:
                return "{0}_{1}_{2}".format(
                    self.name, str(task_id), previous_name
                )

        def check_replace_args(params, task_arguments):
            import re

            new_task_arguments = {}
            for k in task_arguments:
                if re.search(r"(\$.*)", k):
                    if re.findall(r"(\$.*)", k)[0] in params.keys():
                        new_task_arguments[
                            re.sub(
                                r"(\$.*)",
                                params[re.findall(r"(\$.*)", k)[0]],
                                k,
                            )
                        ] = task_arguments[k]
                    else:
                        new_task_arguments[k] = task_arguments[k]
                else:
                    new_task_arguments[k] = task_arguments[k]
            for k in task_arguments:
                if re.search(r"(\$.*)", task_arguments[k]):
                    if (
                        re.findall(r"(\$.*)", task_arguments[k])[0]
                        in params.keys()
                    ):
                        new_task_arguments[k] = re.sub(
                            r"(\$.*)",
                            params[re.findall(r"(\$.*)", task_arguments[k])[0]],
                            task_arguments[k],
                        )
                    else:
                        new_task_arguments[k] = task_arguments[k]
                else:
                    new_task_arguments[k] = task_arguments[k]

            return new_task_arguments

        parameter_check(params, dependencies, name)
        validate_workflow(self, workflow)
        task_id = 1
        all_tasks = []
        non_leaf_tasks = []
        for task in workflow.tasks:
            new_task_name = add_task_name(name, task.name, task_id)
            task_id += 1
            new_arguments = check_replace_args(
                params, task.reverted_arguments()
            )
            new_task = Task(
                operator=task.operator,
                arguments=new_arguments,
                name=new_task_name,
            )
            find_root_tasks_add_dependencies(task, dependencies, new_task)
            add_dependencies(task, new_task, name)
            all_tasks.append(new_task)
            self.addTask(new_task)
            non_leaf_tasks += [t["task"] for t in task.dependencies]

        return [t for t in all_tasks if t.name not in non_leaf_tasks]

    @staticmethod
    def load(file):
        """
        Load a ESDM PAV experiment workflow from the JSON document

        Parameters
        ----------
        file : str
            The name of the ESDM PAV document file to be loaded

        Returns
        -------
        workflow : <class 'esdm_pav_client.workflow.Workflow'>
            Returns the workflow object as it was loaded from the file

        Raises
        ------
        IOError
            Raises IOError if the file does not exist
        JSONDecodeError
            Raises JSONDecodeError if the file does not containt a valid JSON
            structure

        Example
        -------
        w1 = Workflow.load("json_file.json")
        """

        def file_check(filename):
            import os
            import json

            if not os.path.isfile(filename):
                raise IOError("File does not exist")
            else:
                try:
                    with open(filename, "r") as f:
                        return json.loads(f.read())
                except json.decoder.JSONDecodeError:
                    raise ValueError("File is not a valid JSON")

        def check_workflow_name(data):
            if "name" not in data.keys():
                raise AttributeError("Workflow doesn't have a key")

        def start_workflow(data):
            from .task import Task

            workflow = Workflow(name=data["name"])
            del data["name"]
            attrs = {k: data[k] for k in data if k != "name" and k != "tasks"}
            workflow.__dict__.update(attrs)
            for d in data["tasks"]:
                new_task = Task(
                    operator=d["operator"],
                    name=d["name"],
                    arguments={
                        a.split("=")[0]: a.split("=")[1] for a in d["arguments"]
                    },
                )
                new_task.__dict__.update(
                    {
                        k: d[k]
                        for k in d
                        if k != "name" and k != "operator" and k != "arguments"
                    }
                )
                workflow.addTask(new_task)
            return workflow

        data = file_check(file)
        check_workflow_name(data)
        workflow = start_workflow(data)
        return workflow

    def submit(self, *args, server="127.0.0.1", port="11732"):
        """
        Submit an entire ESDM PAV experiment workflow.

        Parameters
        ----------
        server : str, optional
            ESDM PAV runtime DNS/IP address
        port : str, optional
            ESDM PAV runtime port
        args : list
            list of arguments to be substituted in the workflow


        Returns
        -------
        None


        Raises
        ------
        AttributeError
            Raises AttributeError in case of failure to connect to the PAV
            runtime


        Example
        -------
        w1 = Workflow.load("workflow.json")
        w1.submit(server="127.0.0.1", port="11732", "test")
        """
        from PyOphidia import client

        def convert_workflow_to_json():
            import json

            new_workflow = dict(self.__dict__)
            if "tasks" in new_workflow.keys():
                new_workflow["tasks"] = [
                    t.__dict__ for t in new_workflow["tasks"]
                ]
            return json.dumps(new_workflow)

        def param_check(username, server, port, password):
            if not isinstance(username, str):
                raise AttributeError("username must be string")
            if not isinstance(server, str):
                raise AttributeError("server must be string")
            if not isinstance(port, str):
                raise AttributeError("port must be string")
            if not isinstance(password, str):
                raise AttributeError("password must be string")

        username = "oph-test"
        password = "abcd"
        param_check(username, server, port, password)
        po_client = client.Client(
            username=username, password=password, server=server, port=port
        )
        if po_client.last_return_value != 0:
            raise AttributeError("failed to connect to the runtime")
        dict_workflow = convert_workflow_to_json()
        str_workflow = str(dict_workflow)
        po_client.wsubmit(str_workflow, *args)


    def check(self, filename="sample"):
        import tempfile
        import graphviz
        dot = graphviz.Digraph(comment=self.name)
        for task in self.tasks:
            dot.attr('node', shape="circle")
            if task.operator == "oph_if":
                dot.attr('node', shape="rectangle")
            elif task.operator == "oph_for":
                dot.attr('node', shape="hexagon")
            dot.node(task.name, task.name)
            dot.attr('edge', style="solid")
            for d in task.dependencies:
                if "argument" not in d.keys():
                    dot.attr('edge', style="dotted")
                dot.edge(d["task"], task.name)
        dot.render(filename, view=True)
