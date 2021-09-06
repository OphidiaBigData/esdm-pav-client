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
    pyophidia_client = None
    workflow_id = None

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

    @staticmethod
    def _notebook_check():
        try:
            shell = get_ipython().__class__.__name__
            if shell == "ZMQInteractiveShell":
                return True
            elif shell == "TerminalInteractiveShell":
                return False
            else:
                return False
        except NameError:
            return False

    def __runtime_connect(
        self,
        username="oph-test",
        password="abcd",
        server="127.0.0.1",
        port="11732",
    ):
        from PyOphidia import client

        self.__param_check(
            [
                {"name": "username", "value": username, "type": str},
                {"name": "server", "value": server, "type": str},
                {"name": "port", "value": port, "type": str},
                {"name": "password", "value": password, "type": str},
            ]
        )
        if self.pyophidia_client is None:
            self.pyophidia_client = client.Client(
                username=username,
                password=password,
                server=server,
                port=port,
                api_mode=False,
            )
            if self.pyophidia_client.last_return_value != 0:
                raise AttributeError("failed to connect to the runtime")
            else:
                self.pyophidia_client.resume_session()

    def __param_check(self, params=[]):
        for param in params:
            if "NoneValue" in param.keys():
                if (
                    not isinstance(param["value"], param["type"])
                    and param["value"] is not None
                ):
                    raise AttributeError(
                        "{0} should be {1}".format(param["name"], param["type"])
                    )
            else:
                if not isinstance(param["value"], param["type"]):
                    raise AttributeError(
                        "{0} should be {1}".format(param["name"], param["type"])
                    )

    def wokrflow_to_json(self):
        non_workflow_fields = [
            "pyophidia_client",
            "task_name_counter",
            "workflow_id",
        ]
        new_workflow = {
            k: dict(self.__dict__)[k]
            for k in dict(self.__dict__).keys()
            if k not in non_workflow_fields
        }
        if "tasks" in new_workflow.keys():
            new_workflow["tasks"] = [t.__dict__ for t in new_workflow["tasks"]]
        return new_workflow

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
        Retrieve from the ESDM PAV experiment workflow the
        esdm_pav_client.task.Task object with the given task name

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
        data = self.wokrflow_to_json()
        if not workflowname.endswith(".json"):
            workflowname += ".json"
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
        try:
            from task import Task
        except ImportError:
            from .task import Task

        self.__param_check(
            [
                {"name": "operator", "value": operator, "type": str},
                {"name": "arguments", "value": arguments, "type": dict},
                {"name": "dependencies", "value": dependencies, "type": dict},
                {"name": "name", "value": name, "type": str, "NoneValue": True},
            ]
        )
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
        try:
            from task import Task
        except ImportError:
            from .task import Task

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

        self.__param_check(
            [
                {"name": "workflow", "value": workflow, "type": Workflow},
                {"name": "params", "value": params, "type": dict},
                {"name": "dependencies", "value": dependencies, "type": list},
                {"name": "name", "value": name, "type": str, "NoneValue": True},
            ]
        )
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
            try:
                from task import Task
            except ImportError:
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
        import json

        self.exec_mode = "async"
        dict_workflow = json.dumps(self.wokrflow_to_json())
        str_workflow = str(dict_workflow)
        self.__runtime_connect()
        self.pyophidia_client.wsubmit(str_workflow, *args)
        self.exec_mode = "sync"
        self.workflow_id = self.pyophidia_client.last_jobid.split("?")[1].split(
            "#"
        )[0]

    def check(self, filename="sample.dot", visual=True):
        """
        Perform a ESDM PAV experiment workflow validity check and display the
        graph of the workflow

        Parameters
        ----------
        filename  : str, optional
            The name of the file that will contain the diagram

        Returns
        -------
        None

        Example
        -------
        w1 = Workflow(name="Experiment 1", author="sample author",
                       abstract="sample abstract")
        t1 = w1.newTask(operator="oph_reduce", arguments={'operation': 'avg'},
                         dependencies={})
        w1.check("myfile.dot")
        """
        import graphviz

        def _trim_text(text):
            return text[:7] + "..." if len(text) > 10 else text

        def _find_subgraphs(tasks):
            list_of_operators = [t.operator for t in tasks]
            subgraphs_list = [
                {"start_index": start_index, "operator": "if"}
                for start_index in [
                    i for i, t in enumerate(list_of_operators) if t == "if"
                ]
            ]
            subgraphs_list += [
                {"start_index": start_index, "operator": "for"}
                for start_index in [
                    i for i, t in enumerate(list_of_operators) if t == "for"
                ]
            ]
            subgraphs_list = sorted(
                subgraphs_list, key=lambda i: i["start_index"]
            )
            closing_indexes = sorted(
                [
                    i
                    for i, t in enumerate(list_of_operators)
                    if t == "endfor" or t == "endif"
                ]
            )[::-1]
            for i in range(0, len(subgraphs_list)):
                subgraphs_list[i]["end_index"] = closing_indexes[i]

            cluster_counter = 0
            for subgraph in subgraphs_list:
                new_dot = graphviz.Digraph(
                    name="cluster_{0}".format(str(cluster_counter))
                )
                for i in range(
                    subgraph["start_index"], subgraph["end_index"] + 1
                ):
                    new_dot.attr("node")
                    new_dot.node(
                        tasks[i].name,
                        _trim_text(tasks[i].name)
                        + "\n"
                        + _trim_text(tasks[i].type)
                        + "\n"
                        + _trim_text(tasks[i].operator),
                    )
                subgraph["dot"] = new_dot
                cluster_counter += 1
            return subgraphs_list

        def _check_workflow_validity():
            import json

            self.__runtime_connect()
            workflow_validity = self.pyophidia_client.wisvalid(
                json.dumps(self.wokrflow_to_json())
            )
            if workflow_validity[1] == "Workflow is valid":
                return True
            else:
                return False

        workflow_validity = _check_workflow_validity()
        self.__param_check(
            [
                {"name": "filename", "value": filename, "type": str},
                {"name": "visual", "value": visual, "type": bool},
            ]
        )
        if visual is False:
            return workflow_validity
        diamond_commands = ["if", "endif", "else"]
        hexagonal_commands = ["for", "endfor"]
        dot = graphviz.Digraph(comment=self.name)
        for task in self.tasks:
            dot.attr(
                "node", shape="circle", width="1", penwidth="1", fontsize="10pt"
            )
            dot.attr("edge", penwidth="1")
            if task.operator in diamond_commands:
                dot.attr("node", shape="diamond")
            elif task.operator in hexagonal_commands:
                dot.attr("node", shape="hexagon")
            dot.node(
                task.name,
                _trim_text(task.name)
                + "\n"
                + _trim_text(task.type)
                + "\n"
                + _trim_text(task.operator),
            )
            dot.attr("edge", style="solid")
            for d in task.dependencies:
                if "argument" not in d.keys():
                    dot.attr("edge", style="dashed")
                dot.edge(d["task"], task.name)
        subgraphs = _find_subgraphs(self.tasks)
        if len(subgraphs) > 1:
            for i in range(0, len(subgraphs) - 1):
                subgraphs[i]["dot"].subgraph(subgraphs[i + 1]["dot"])
            dot.subgraph(subgraphs[0]["dot"])
        notebook_check = self._notebook_check()
        if notebook_check is True:
            # TODO change the image dimensions
            from IPython.display import display

            display(dot)
        else:
            dot.render(filename, view=True)

    def cancel(self):
        """
        Cancel the ESDM PAV experiment workflow that has been submitted

        Returns
        -------
        None

        Example
        -------
        w1 = Workflow(name="Experiment 1", author="sample author",
                      abstract="sample abstract")
        t1 = w1.newTask(operator="oph_reduce", arguments={'operation': 'avg'},
                        dependencies={})
        w1.submit()
        w1.cancel()
        """
        self.__runtime_connect()
        self.pyophidia_client.submit(
            query="oph_cancel id={0};exec_mode=async;".format(self.workflow_id)
        )

    def monitor(self, frequency=10, iterative=True, visual_mode=True):
        """
        Monitors the progress of the ESDM PAV experiment workflow execution

        Parameters
        ----------
        frequency : int
            The frequency in seconds to receive the updates
        iterative: bool
            True for receiving updates periodically, based on the frequency, or
            False to receive updates only once
        visual_mode: bool
            True for receiving the workflow status as an image or False to
            receive updates only in text

        Returns
        -------
        workflow_status : <class 'str'>
            Returns the workflow status as a string

        Raises
        ------
        AttributeError
            Raises AttributeError when workflow is not valid

        Example
        -------
         w1 = Workflow(name="Experiment 1", author="sample author",
                        abstract="sample abstract")
         t1 = w1.newTask(operator="oph_reduce", arguments={'operation': 'avg'},
                          dependencies={})
         w1.__runtime_connect()
         w1.submit()
         w1.monitor(frequency=100, iterative=True, visual_mode=True)
        """
        import graphviz
        import json
        import time
        import re

        def _trim_text(text):
            return text[:7] + "..." if len(text) > 10 else text

        def _find_matches(d, item):
            for k in d:
                if re.match(k, item):
                    return d[k]

        def _check_workflow_validity():
            import json

            self.__runtime_connect()
            workflow_validity = self.pyophidia_client.wisvalid(
                json.dumps(self.wokrflow_to_json())
            )
            if not workflow_validity[1] == "Workflow is valid":
                raise AttributeError("Workflow is not valid")

        def _find_subgraphs(tasks):
            list_of_operators = [t.operator for t in tasks]
            subgraphs_list = [
                {"start_index": start_index, "operator": "if"}
                for start_index in [
                    i for i, t in enumerate(list_of_operators) if t == "if"
                ]
            ]
            subgraphs_list += [
                {"start_index": start_index, "operator": "for"}
                for start_index in [
                    i for i, t in enumerate(list_of_operators) if t == "for"
                ]
            ]
            subgraphs_list = sorted(
                subgraphs_list, key=lambda i: i["start_index"]
            )
            closing_indexes = sorted(
                [
                    i
                    for i, t in enumerate(list_of_operators)
                    if re.match("(?i).*endfor", t) or re.match("(?i).*endif", t)
                ]
            )[::-1]
            for i in range(0, len(subgraphs_list)):
                subgraphs_list[i]["end_index"] = closing_indexes[i]

            cluster_counter = 0
            for subgraph in subgraphs_list:
                new_dot = graphviz.Digraph(
                    name="cluster_{0}".format(str(cluster_counter))
                )
                for i in range(
                    subgraph["start_index"], subgraph["end_index"] + 1
                ):
                    new_dot.attr("node")
                    new_dot.node(
                        tasks[i].name,
                        _trim_text(tasks[i].name)
                        + "\n"
                        + _trim_text(tasks[i].type)
                        + "\n"
                        + _trim_text(tasks[i].operator),
                    )
                subgraph["dot"] = new_dot
                cluster_counter += 1
            return subgraphs_list

        def _check_workflow_status(json_response):
            for res in json_response["response"]:
                if res["objkey"] == "workflow_status":
                    return res["objcontent"][0]["message"]

        def _extract_info(json_response):
            task_dict = {}
            for res in json_response["response"]:
                if res["objkey"] == "workflow_list":
                    task_name_index = res["objcontent"][0]["rowkeys"].index(
                        "TASK NAME"
                    )
                    status_index = res["objcontent"][0]["rowkeys"].index(
                        "EXIT STATUS"
                    )
                    for task in res["objcontent"][0]["rowvalues"]:
                        task_dict[task[int(task_name_index)]] = task[
                            int(status_index)
                        ]
            return task_dict

        def _match_shapes(operator, commands):
            for command in commands:
                if re.match("(?i).*"+command, operator):
                    return True
            return False

        def _sort_tasks(tasks):
            sorted_tasks = []
            for i in range (0, len(tasks)):
                if re.findall(r".*?(\([0-9].*\))", tasks[i].name):
                    clean_name = tasks[i].name.replace(
                        re.findall(r".*?(\([0-9].*\))", tasks[i].name)[0], "")
                    for task in tasks[i:]:
                        if clean_name in task.name and task.name not in [t.name for t in sorted_tasks] and re.findall(r".*?(\([0-9].*\))", task.name):
                            sorted_tasks.append(task)
                else:
                    sorted_tasks.append(tasks[i])
            return sorted_tasks



        def _modify_task(json_response):
            try:
                from task import Task
            except ImportError:
                from .task import Task

            new_tasks = []
            for res in json_response["response"]:
                if res["objkey"] == "resume":
                    task_name_index = res["objcontent"][0]["rowkeys"].index("COMMAND")
                    tasks = json.loads(res["objcontent"][0]["rowvalues"][0][task_name_index])
            for task in tasks["tasks"]:
                arguments = {}
                for j in task["arguments"]:
                    arguments[j.split("=")[0]] = j.split("=")[1]
                task_obj = Task(name=task["name"], operator=task["operator"], arguments=arguments)
                if "dependencies" in task.keys():
                    for dependency in task["dependencies"]:
                        task_obj.copyDependency(dependency)
                new_tasks.append(task_obj)
            return new_tasks

        def _draw(tasks, json_response, status_color_dictionary=None, ):
            task_dict = _extract_info(json_response)
            diamond_commands = ["if", "endif", "else"]
            hexagonal_commands = ["for", "endfor"]
            dot = graphviz.Digraph(comment=self.name)
            for task in tasks:
                dot.attr(
                    "node",
                    shape="circle",
                    width="1",
                    penwidth="1",
                    style="",
                    fontsize="10pt",
                )
                if len(task_dict.keys()) == 0 and task == self.tasks[0]:
                    dot.attr("node", fillcolor="red", style="filled")
                if task.name in task_dict and status_color_dictionary:
                    dot.attr(
                        "node",
                        fillcolor=_find_matches(
                            status_color_dictionary, task_dict[task.name]
                        ),
                        style="filled",
                    )
                dot.attr("edge", penwidth="1")
                if _match_shapes(task.operator, diamond_commands):
                    dot.attr("node", shape="diamond")
                elif _match_shapes(task.operator, hexagonal_commands):
                    dot.attr("node", shape="hexagon")
                dot.node(
                    task.name,
                    _trim_text(task.name)
                    + "\n"
                    + _trim_text(task.type)
                    + "\n"
                    + _trim_text(task.operator),
                )
                dot.attr("edge", style="solid")
                for d in task.dependencies:
                    if "argument" not in d.keys():
                        dot.attr("edge", style="dashed")
                    dot.edge(d["task"], task.name)
            subgraphs = _find_subgraphs(tasks)
            for i in range(0, len(subgraphs) - 1):
                subgraphs[i]["dot"].subgraph(subgraphs[i + 1]["dot"])
            if len(subgraphs) > 0:
                dot.subgraph(subgraphs[0]["dot"])
            notebook_check = self._notebook_check()
            if notebook_check is True:
                # TODO change the image dimensions
                from IPython.display import display, clear_output

                clear_output(wait=True)
                display(dot)
            else:
                dot.render("sample", view=True)

        self.__param_check(
            params=[
                {"name": "frequency", "value": frequency, "type": int},
                {"name": "iterative", "value": iterative, "type": bool},
                {"name": "visual_mode", "value": visual_mode, "type": bool},
            ]
        )
        status_color_dictionary = {
            "(?i).*RUNNING": "orange",
            "(?i).*UNSELECTED": "grey",
            "(?i).*UNKNOWN": "grey",
            "(?i).*PENDING": "pink",
            "(?i).*WAITING": "cyan",
            "(?i).*COMPLETED": "palegreen1",
            "(?i).*ERROR": "red",
            "(.*?)_ERROR": "red",
            "(?i).*ABORTED": "red",
            "(?i).*SKIPPED": "yellow",
        }
        _check_workflow_validity()
        self.__runtime_connect()
        self.pyophidia_client.submit(
            "oph_resume id={0};".format(self.workflow_id)
        )
        status_response = json.loads(self.pyophidia_client.last_response)
        self.pyophidia_client.submit(
            "oph_resume document_type=request;level=3;id={0};".format(self.workflow_id)
        )
        json_response = json.loads(self.pyophidia_client.last_response)

        task_dict = {}
        tasks = _modify_task(json_response)
        sorted_tasks = _sort_tasks(tasks)
        workflow_status = _check_workflow_status(status_response)
        if iterative is True:
            while True:
                if visual_mode is True:
                    _draw(sorted_tasks, status_response, status_color_dictionary)
                else:
                    print(workflow_status)
                if (not re.match("(?i).*RUNNING", workflow_status)
                    and
                    (not re.match("(?i).*PENDING", workflow_status))

                ):
                    return workflow_status
                time.sleep(frequency)
                self.pyophidia_client.submit(
                    "oph_resume id={0};".format(self.workflow_id)
                )
                status_response = json.loads(self.pyophidia_client.last_response)
                workflow_status = _check_workflow_status(status_response)
        else:
            if visual_mode is True:
                _draw(sorted_tasks, json_response, status_color_dictionary)
                return workflow_status
            else:
                print(workflow_status)
                return workflow_status
