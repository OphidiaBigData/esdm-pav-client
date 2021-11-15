class Experiment:
    """
    Creates or loads an ESDM PAV experiment.

    An experiment is a sequence of tasks. Each task can be either independent
    or dependent on other tasks, for instance it processes the output of other
    tasks.

    Construction::
    e1 = Experiment(name="sample", author="sample author",
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
    subexperiment_names = []
    pyophidia_client = None
    username = "oph-test"
    password = "abcd"
    server = "127.0.0.1"
    port = "11732"

    def __init__(self, name, author=None, abstract=None, **kwargs):
        for k in kwargs.keys():
            if k not in self.attributes:
                raise AttributeError(
                    "Unknown experiment argument: {0}".format(k))
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

    def __runtime_connect(self):
        from PyOphidia import client

        self.__param_check(
            [
                {"name": "username", "value": self.username, "type": str},
                {"name": "server", "value": self.server, "type": str},
                {"name": "port", "value": self.port, "type": str},
                {"name": "password", "value": self.password, "type": str},
            ]
        )
        if self.pyophidia_client is None:
            self.pyophidia_client = client.Client(
                username=self.username,
                password=self.password,
                server=self.server,
                port=self.port,
                api_mode=False,
            )
            if self.pyophidia_client.last_return_value != 0:
                raise AttributeError("failed to connect to the runtime")
            else:
                self.pyophidia_client.resume_session()

    def __param_check(self, params=[]):
        for param in params:
            if "NoneValue" in param.keys():
                if not isinstance(param["value"], param["type"]) \
                        and param["value"] is not None:
                    raise AttributeError("{0} should be {1}"
                                         .format(param["name"], param["type"]))
            else:
                if not isinstance(param["value"], param["type"]):
                    raise AttributeError("{0} should be {1}"
                                         .format(param["name"], param["type"]))

    def wokrflow_to_json(self):
        non_experiment_fields = [
            "pyophidia_client",
            "task_name_counter"
        ]
        new_experiment = {k: dict(self.__dict__)[k] for k in
                          dict(self.__dict__).keys() if
                          k not in non_experiment_fields}
        if "tasks" in new_experiment.keys():
            new_experiment["tasks"] = [t.__dict__ for t in
                                       new_experiment["tasks"]]
        return new_experiment

    def deinit(self):
        """
        Reverse the initialization of the object
        """
        for k in self.active_attributes:
            self.__delattr__(k)

    def addTask(self, task):
        """
        Adds a task to the ESDM PAV experiment

        Parameters
        ----------
        task : <class 'esdm_pav_client.task.Task'>
            Task to be added to the experiment

        Raises
        ------
        AttributeError
            If the task name is already in the experiment or if a dependency is
            not fulfilled

        Example
        -------
        t1 = Task(name="sample task", operator='oph_reduce',
                    arguments={'operation': 'avg'})
        e1.addTask(t1)
        """
        if "name" not in task.__dict__.keys() or task.name is None:
            task.name = self.name + "_{0}".format(self.task_name_counter)
        if task.__dict__["name"] in [t.__dict__["name"] for t in self.tasks]:
            raise AttributeError("task already exists")
        if task.__dict__["dependencies"]:
            for dependency in task.__dict__["dependencies"]:
                if dependency["task"] not in [task.__dict__["name"] for task in
                                              self.tasks]:
                    raise AttributeError("dependency not fulfilled")
        self.task_name_counter += 1
        self.tasks.append(task)

    def getTask(self, taskname):
        """
        Retrieve from the ESDM PAV experiment the
        esdm_pav_client.task.Task object with the given task name

        Parameters
        ----------
        taskname : str
            The name of the task to be found in the experiment

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
        task = e1.getTask(taskname="task_one")
        """
        tasks = [t for t in self.tasks if t["name"] == taskname]
        if len(tasks) == 1:
            return tasks[0]
        elif len(tasks) == 0:
            return None

    def save(self, experimentname):
        """
        Save the ESDM PAV experiment as a JSON document

        Parameters
        ----------
        experimentname : str
            The path to the ESDM PAV document file where the experiment is
            being
            saved

        Example
        -------
        from esdm_pav_client import experiment
        e1 = experiment(name="sample name", author="sample author",
                        abstract="sample abstract")
        e1.save("sample_experiment")

        Raises
        ------
        AttributeError
            If worfklowname is not a string or it is empty
        """
        import json
        import os

        if not isinstance(experimentname, str):
            raise AttributeError("experimentname must be string")
        if len(experimentname) == 0:
            raise AttributeError(
                "experimentname must contain more than 1 characters")
        data = self.wokrflow_to_json()
        if not experimentname.endswith(".json"):
            experimentname += ".json"
        with open(os.path.join(os.getcwd(), experimentname), "w") as fp:
            json.dump(data, fp, indent=4)

    def newTask(self, operator, arguments={}, dependencies={}, name=None,
                **kwargs):
        """
        Adds a new Task in the ESDM PAV experiment without the need
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
            Returns the task that was created and added to the experiment

        Raises
        ------
        AttributeError
            Raises an AttributeError if the given arguments are not of the
            proper type or are not defined by the schema

        Example
        -------
        e1 = Experiment(name="Experiment 1", author="sample author",
                        abstract="sample abstract")
        t1 = e1.newTask(operator="oph_reduce", arguments={'operation': 'avg'},
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
                {"name": "name", "value": name, "type": str,
                 "NoneValue": True},
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

    def newSubexperiment(self, experiment, params, dependencies={}, name=None):
        """
        Embeds an ESDM PAV experiment into another experiment

        Parameters
        ----------
        experiment : <class 'esdm_pav_client.experiment.experiment'>
            The experiment that will be embeded into our main experiment
        params : dict of keywords
            a dict of keywords that will be used to replace placeholders in
            the tasks
        dependencies : list, optional
            list of dependencies
        name : str, optional
            a unique name for the experiment's tasks

        Returns
        -------
        A list of the leaf tasks of the subexperiment

        Raises
        ------
        AttributeError
            Raises AttributeError when there's an error with the experiments
            (same name or non-existent), or when the dependencies are not
            fulfilled

        Example
        -------
        e1 = experiment(name="Experiment 1", author="sample author 1",
                        abstract="sample abstract 1")
        e2 = experiment(name="Experiment 2", author="sample author 2",
                        abstract="sample abstract 2")
        t1 = e2.newTask(operator='oph_reduce', arguments={'operation': 'avg'})
        task_array = e1.newSubexperiment(name="new_subexperiment",
        experiment=e2,
                                         params={}, dependencies=[])
        """
        try:
            from task import Task
        except ImportError:
            from .task import Task

        def validate_experiment(e1, e2):
            if not isinstance(e2, Experiment) or e1.name == e2.name:
                raise AttributeError("Wrong experiment or same experiments")

        def dependency_check(dependency):
            if not isinstance(dependency, dict):
                raise AttributeError("dependency must be a list")
            if len(dependency.keys()) > 2:
                raise AttributeError("Wrong dependency arguments")
            elif len(dependency.keys()) == 2:
                if ("task" not in dependency.keys()) or (
                        "argument" not in dependency.keys()):
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
                return "{0}_{1}_{2}".format(self.name, str(task_id),
                                            previous_name)

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
                    if re.findall(r"(\$.*)", task_arguments[k])[
                        0] in params.keys():
                        new_task_arguments[k] = re.sub(
                            r"(\$.*)",
                            params[
                                re.findall(r"(\$.*)", task_arguments[k])[0]],
                            task_arguments[k],
                        )
                    else:
                        new_task_arguments[k] = task_arguments[k]
                else:
                    new_task_arguments[k] = task_arguments[k]

            return new_task_arguments

        self.__param_check(
            [
                {"name": "experiment", "value": experiment,
                 "type": Experiment},
                {"name": "params", "value": params, "type": dict},
                {"name": "dependencies", "value": dependencies, "type": list},
                {"name": "name", "value": name, "type": str,
                 "NoneValue": True},
            ]
        )
        validate_experiment(self, experiment)
        task_id = 1
        all_tasks = []
        non_leaf_tasks = []
        for task in experiment.tasks:
            new_task_name = add_task_name(name, task.name, task_id)
            task_id += 1
            new_arguments = check_replace_args(params,
                                               task.reverted_arguments())
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
        Load a ESDM PAV experiment from the JSON document

        Parameters
        ----------
        file : str
            The name of the ESDM PAV document file to be loaded

        Returns
        -------
        experiment : <class 'esdm_pav_client.experiment.Experiment'>
            Returns the experiment object as it was loaded from the file

        Raises
        ------
        IOError
            Raises IOError if the file does not exist
        JSONDecodeError
            Raises JSONDecodeError if the file does not containt a valid JSON
            structure

        Example
        -------
        e1 = Experiment.load("json_file.json")
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

        def check_experiment_name(data):
            if "name" not in data.keys():
                raise AttributeError("experiment doesn't have a key")

        def start_experiment(data):
            try:
                from task import Task
            except ImportError:
                from .task import Task

            experiment = Experiment(name=data["name"])
            del data["name"]
            attrs = {k: data[k] for k in data if k != "name" and k != "tasks"}
            experiment.__dict__.update(attrs)
            for d in data["tasks"]:
                new_task = Task(
                    operator=d["operator"],
                    name=d["name"],
                    arguments={a.split("=")[0]: a.split("=")[1] for a in
                               d["arguments"]},
                )
                new_task.__dict__.update({k: d[k] for k in d if
                                          k != "name" and k != "operator"
                                          and k != "arguments"})
                experiment.addTask(new_task)
            return experiment

        data = file_check(file)
        check_experiment_name(data)
        experiment = start_experiment(data)
        return experiment

    def check(self, filename="sample.dot", visual=True):
        """
        Perform a ESDM PAV experiment validity check and display the
        graph of the experiment

        Parameters
        ----------
        filename  : str, optional
            The name of the file that will contain the diagram

        Returns
        -------
        None

        Example
        -------
        e1 = Experiment(name="Experiment 1", author="sample author",
                       abstract="sample abstract")
        t1 = e1.newTask(operator="oph_reduce", arguments={'operation': 'avg'},
                         dependencies={})
        e1.check("myfile.dot")
        """
        import graphviz

        def _trim_text(text):
            return text[:7] + "..." if len(text) > 10 else text

        def _find_subgraphs(tasks):
            list_of_operators = [t.operator for t in tasks]
            subgraphs_list = [{"start_index": start_index, "operator": "if"}
                              for start_index in
                              [i for i, t in enumerate(list_of_operators) if
                               t == "if"]]
            subgraphs_list += [{"start_index": start_index, "operator": "for"}
                               for start_index in
                               [i for i, t in enumerate(list_of_operators) if
                                t == "for"]]
            subgraphs_list = sorted(subgraphs_list,
                                    key=lambda i: i["start_index"])
            closing_indexes = sorted(
                [i for i, t in enumerate(list_of_operators) if
                 t == "endfor" or t == "endif"])[::-1]
            for i in range(0, len(subgraphs_list)):
                subgraphs_list[i]["end_index"] = closing_indexes[i]

            cluster_counter = 0
            for subgraph in subgraphs_list:
                new_dot = graphviz.Digraph(
                    name="cluster_{0}".format(str(cluster_counter)))
                for i in range(subgraph["start_index"],
                               subgraph["end_index"] + 1):
                    new_dot.attr("node")
                    new_dot.node(
                        tasks[i].name,
                        _trim_text(tasks[i].name) + "\n" + _trim_text(
                            tasks[i].type) + "\n" + _trim_text(
                            tasks[i].operator),
                    )
                subgraph["dot"] = new_dot
                cluster_counter += 1
            return subgraphs_list

        def _check_experiment_validity():
            import json

            self.__runtime_connect()
            experiment_validity = self.pyophidia_client.wisvalid(
                json.dumps(self.wokrflow_to_json()))
            if experiment_validity[1] == "experiment is valid":
                return True
            else:
                return False

        experiment_validity = _check_experiment_validity()
        self.__param_check(
            [
                {"name": "filename", "value": filename, "type": str},
                {"name": "visual", "value": visual, "type": bool},
            ]
        )
        if visual is False:
            return experiment_validity
        diamond_commands = ["if", "endif", "else"]
        hexagonal_commands = ["for", "endfor"]
        dot = graphviz.Digraph(comment=self.name)
        for task in self.tasks:
            dot.attr("node", shape="circle", width="1", penwidth="1",
                     fontsize="10pt")
            dot.attr("edge", penwidth="1")
            if task.operator in diamond_commands:
                dot.attr("node", shape="diamond")
            elif task.operator in hexagonal_commands:
                dot.attr("node", shape="hexagon")
            dot.node(
                task.name,
                _trim_text(task.name) + "\n" + _trim_text(
                    task.type) + "\n" + _trim_text(task.operator),
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

    def newWaitTask(self, name=None, type=None, output=None,
                    measure=None, subset_dims=None,
                    subset_filter=None, subset_type="coord",
                    timeout="10"):
        """

        Parameters
        ----------
        name: str, optional
            The name of the task. Default value is "name"
        type: str, optional
            The type of the argument. Default value is "file"
        output: str, optional
            The output file location. Default value is "esdm://etos.nc"
        measure: str, optional
            The measure variable. Default value is "tos"
        subset_dims: str, optional
            The subset dimension. Default value is "time"
        subset_filter: str, optional
            The subset filter. Default value is "2001-05_2001-06"
        subset_type: str, optional
            The subset type. Default value is "coord"
        timeout: str, optional
            The timeout value. Default value is "10"

        Raises
        ------
        AttributeError
            If a provided argument is of wrong type.

        Returns
        -------

        """
        self.__param_check(
            [
                {"name": "name", "value": name, "type": str,
                 "NoneValue": True},
                {"name": "type", "value": type, "type": str,
                 "NoneValue": True},
                {"name": "output", "value": output, "type": str,
                 "NoneValue": True},
                {"name": "measure", "value": measure, "type": str,
                 "NoneValue": True},
                {"name": "subset_dims", "value": subset_dims, "type": str,
                 "NoneValue": True},
                {"name": "subset_filter", "value": subset_filter, "type": str,
                 "NoneValue": True},
                {"name": "subset_type", "value": subset_type, "type": str,
                 "NoneValue": True},
                {"name": "timeout", "value": timeout, "type": str,
                 "NoneValue": True}

            ]
        )
        if name is None:
            name = "name"
        if type == "file":
            if output is None:
                raise ValueError("If type is set to file you would have to "
                                 "provide an output argument")

        self.newTask(name=name, type="control", operator="wait",
                     arguments={"type": type, "output": output,
                                "measure": measure,
                                "subset_dims": subset_dims,
                                "subset_filter": subset_filter,
                                "subset_type": subset_type,
                                "timeout": timeout})

    def newIfTask(self, condition, if_branch, else_branch):
        """
        Parameters
        ----------
        condition: str
            The condition for the "if" statement
        if_branch: Experiment
            An Experiment object that will be used in order execute its tasks
            in case the "if" statement is True.
        else_branch: Experiment
            An Experiment object that will be used in order execute its tasks
            in case the "if" statement is False.

        Raises
        ------
        AttributeError
            If a provided argument is of wrong type or .

        Returns
        -------

        """
        self.__param_check([{"name": "condition", "value": condition,
                             "type": str},
                            {"name": "if_branch", "value": if_branch,
                             "type": Experiment},
                            {"name": "else_branch", "value": else_branch,
                             "type": Experiment}])
        if len(if_branch.tasks) == 0 or len(else_branch.tasks) == 0:
            raise AttributeError("You have to add tasks to the if_branch "
                                 "Experiment")
        t1 = self.newTask(name="Begin selection",
                          type="control",
                          operator='if',
                          arguments={"condition": condition})
        for task in if_branch.tasks:
            self.tasks.append(task)
        t3 = self.newTask(name="Else",
                          type="control",
                          operator='else',
                          dependencies={t1: ''})
        for task in else_branch.tasks:
            self.tasks.append(task)
        t7 = self.newTask(name="End selection",
                          type="control",
                          operator='endif',
                          arguments={},
                          dependencies={if_branch.tasks[-1]: "",
                                        else_branch.tasks[-1]: ""})

    def newForTask(self, name="loop", index_values="$1", parallel="yes",
                   iteration_branch=None):
        """
        Parameters
        ----------
        name: str
            The name of the starting task
        index_values: str
            The index_values that will be used as arguments
        parallel: str
            Set parallel to yes if you want the tasks inside the for loop to
            run in parallel mode.
        iteration_branch: Experiment
            An Experiment object that will be used in order execute its tasks
            inside the for loop.

        Raises
        ------
        AttributeError
            If a provided argument is of wrong type or .

        Returns
        -------

        """
        self.__param_check([{"name": "name", "value": name,
                             "type": str},
                            {"name": "index_values", "value": index_values,
                             "type": str},
                            {"name": "parallel", "value": parallel,
                             "type": str},
                            {"name": "iteration_branch",
                             "value": iteration_branch,
                             "type": Experiment}
                            ])
        if len(iteration_branch.tasks) == 0:
            raise AttributeError("You have to add tasks to the "
                                 "iteration_branch Experiment")
        self.newTask(name=name,
                     type="control",
                     operator='for',
                     arguments={"key": "index", "values": index_values,
                                "parallel": parallel})
        for task in iteration_branch.tasks:
            self.tasks.append(task)
        self.newTask(name="End loop",
                     type="control",
                     operator='endfor',
                     arguments={},
                     dependencies={iteration_branch.tasks[-1]: 'cube'})
