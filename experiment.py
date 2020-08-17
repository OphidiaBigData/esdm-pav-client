class Workflow:
    """Workflow(name, author=None, abstract=None, **kwargs)
    Attributes:
        name: workflow name
        author: workflow author
        abstract: workflow description

    Keyword Args:
        url: workflow URL
        sessionid: session id for the entire workflow
        exec_mode: execution mode of the workflow, sync or async
        ncores: number of cores
        nhost: number of hosts
        on_error: behaviour in case of error
        on_exit: operation to be executed on output objects
        run: enable submission to analytics framework, yes or no
        cwd: current working directory
        cdd: absolute path corresponding to the current directory on data repository
        cube: cube PID for the entire workflow
        callback_url: callback URL for the entire workflow
        output_format: mode to code workflow output
        host_partition: name of host partition to be used in the workflow
        nthreads: number of threads
    """
    attributes = ["url", "sessionid", "exec_mode", "ncores", "nhost", "on_error", "on_exit", "run", "cwd", "cdd",
                  "cube", "callback_url", "output_format", "host_partition", "nthreads"]
    active_attributes = ["name", "author", "abstract"]
    task_attributes = ["run", "on_exit", "on_error"]
    task_name_counter = 1
    subworkflow_names = []

    def __init__(self, name, author=None, abstract=None, **kwargs):
        """Workflow(name, author=None, abstract=None, **kwargs)
        :param name: workflow name
        :type name: <class 'str'>
        :param author: workflow author
        :type author: <class 'str'>
        :param abstract: workflow description
        :type abstract: <class 'str'>
        :key url: workflow URL
        :type url: <class 'str'>
        :key sessionid: session id for the entire workflow
        :type sessionid: <class 'str'>
        :key exec_mode: execution mode of the workflow, sync or async
        :type exec_mode: <class 'str'>
        :key ncores: number of cores
        :type ncores: <class 'int'>
        :key nhost: number of hosts
        :type nhost: <class 'int'>
        :key on_error: behaviour in case of error
        :type on_error: <class 'str'>
        :key on_exit: operation to be executed on output objects
        :type on_exit: <class 'str'>
        :key run: enable submission to analytics framework, yes or no
        :type run: <class 'str'>
        :key cwd: current working directory
        :type cwd: <class 'str'>
        :key cdd: absolute path corresponding to the current directory on data repository
        :type cdd: <class 'str'>
        :key cube: cube PID for the entire workflow
        :type cube: <class 'str'>
        :key callback_url: callback URL for the entire workflow
        :type callback_url: <class 'str'>
        :key output_format: mode to code workflow output
        :type output_format: <class 'str'>
        :key host_partition: name of host partition to be used in the workflow
        :type host_partition: <class 'str'>
        :key nthreads: workflow number of threads
        :type nthreads: <class 'int'>
        """
        for k in kwargs.keys():
            if k not in self.attributes:
                raise AttributeError("Unknown workflow argument: {0}".format(k))
            self.active_attributes.append(k)
        self.name = name
        self.author = author
        self.abstract = abstract
        self.tasks = []
        self.__dict__.update(kwargs)

    def deinit(self):
        """deinit() -> None : reverse the initialization of the object
        :return: None
        :rtype: None
        """
        for k in self.active_attributes:
            self.__delattr__(k)

    def addTask(self, task):
        """addTask(task) -> None : adds tasks to workflow
        :param task: task
        :type task: <class 'pav.Task'>
        :returns: None
        :rtype: None
        :raises: AttributeError
        """
        if "name" not in task.__dict__.keys() or task.name is None:
            task.name = self.name + "_{0}".format(self.task_name_counter)
        if task.__dict__["name"] in [t.__dict__["name"] for t in self.tasks]:
            raise AttributeError("task already exists")
        if task.__dict__["dependencies"]:
            for dependency in task.__dict__["dependencies"]:
                # print([task.__dict__["name"] for task in self.tasks])
                if dependency["task"] not in [task.__dict__["name"] for task in self.tasks]:
                    raise AttributeError("dependency not fulfilled")
        self.task_name_counter += 1
        # self.tasks.append(task.__dict__)
        self.tasks.append(task)

    def getTask(self, taskname):
        """getTask(taskname) -> pav.Task|None : get Task from a given string
        :param taskname: name of the task
        :type taskname: <class 'str'>
        :returns: pav.Task | None
        :rtype: <class 'pav.Task'> | <class 'NoneType'>
        """
        tasks = [t for t in self.tasks if t["name"] == taskname]
        if len(tasks) == 1:
            return tasks[0]
        elif len(tasks) == 0:
            return None

    def save(self, workflowname):
        """save(workflowname) -> None : save the workflow in a file
        :param workflowname:
        :type workflowname: <class 'str'>
        :return: None
        """
        import json
        import os
        data = self.__dict__
        del data["task_name_counter"]
        if not workflowname.endswith(".json"):
            workflowname += ".json"
        self.tasks = [t.__dict__ for t in self.tasks]
        with open(os.path.join(os.getcwd(), workflowname), 'w') as fp:
            json.dump(data, fp, indent=4)

    def newTask(self, operator, arguments={}, dependencies={}, name=None, **kwargs):
        """newTask(operator, arguments, dependencies, name, **kwargs) -> pav.Task : add new Task in workflow
        :param operator: Ophidia operator name
        :type operator: <class 'str'>
        :param arguments: list of user-defined operator arguments as key=value pairs
        :type arguments: <class 'dict'>
        :param dependencies:
        :type dependencies: <class 'dict'>
        :param name: name of the task
        :type name: <class 'str'>
        :key on_error: workflow description
        :type on_error: <class 'str'>
        :key on_exit: workflow description
        :type on_exit: <class 'str'>
        :key run: workflow description
        :type run: <class 'str'>
        :return: pavtask
        :rtype: <class 'pav.Task'>
        :raises: AttributeError
        """
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

    def newSubWorkflow(self, workflow, params, dependency=[], name=None):
        """newSubWorkflow(workflow, params, dependency=[], name=None) -> list : combines two workflows and returns a
        task list
        :param workflow: workflow to be embedded
        :type workflow: <class 'pav.Workflow'>
        :param params: dict of keywords to be replaced
        :type params: <class 'dict'>
        :param dependency: list of dependencies
        :type dependency: <class 'list'>
        :param name: unique name for the workflow's tasks
        :type name: <class 'str'>
        :return: list
        :rtype: <class 'list'>
        :raises: AttributeError
        """
        def validate_workflow(w1, w2):
            if not isinstance(w2, Workflow) or w1.name == w2.name:
                raise AttributeError("Wrong workflow or same workflows")

        def dependency_check(dependency):
            if len(dependency.keys()) > 2:
                raise AttributeError("Wrong dependency arguments")
            elif len(dependency.keys()) == 2:
                if ("task" not in dependency.keys()) or ("argument" not in dependency.keys()):
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
                        new_task.addDependency(task=dependency["task"], argument=dependency["argument"])

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
                return "{0}_{1}_{2}".format(self.name, str(task_id), previous_name)

        def check_replace_args(params, task_arguments):
            import re
            new_task_arguments = {}
            for k in task_arguments:
                if re.search('(\$.*)', k):
                    if re.findall('(\$.*)', k)[0] in params.keys():
                        new_task_arguments[re.sub("(\$.*)", params[re.findall('(\$.*)', k)[0]], k)] = task_arguments[k]
                    else:
                        new_task_arguments[k] = task_arguments[k]
                else:
                    new_task_arguments[k] = task_arguments[k]
            for k in task_arguments:
                if re.search('(\$.*)', task_arguments[k]):
                    if re.findall('(\$.*)', task_arguments[k])[0] in params.keys():
                        new_task_arguments[k] = re.sub("(\$.*)", params[re.findall('(\$.*)', task_arguments[k])[0]],
                                                       task_arguments[k])
                    else:
                        new_task_arguments[k] = task_arguments[k]
                else:
                    new_task_arguments[k] = task_arguments[k]

            return new_task_arguments

        validate_workflow(self, workflow)
        task_id = 1
        all_tasks = []
        non_leaf_tasks = []
        for task in workflow.tasks:
            new_task_name = add_task_name(name, task.name, task_id)
            task_id += 1
            new_arguments = check_replace_args(params, task.reverted_arguments())
            new_task = Task(operator=task.operator, arguments=new_arguments, name=new_task_name)
            find_root_tasks_add_dependencies(task, dependency, new_task)
            add_dependencies(task, new_task, name)
            all_tasks.append(new_task)
            self.addTask(new_task)
            non_leaf_tasks += [t["task"] for t in task.dependencies]

        return [t for t in all_tasks if t.name not in non_leaf_tasks]

    @staticmethod
    def load(file):
        """load(file) -> None : loads a workflow from a JSON file
        :param file: the name of the JSON file
        :type file: <class 'str'>
        :return: None
        :raises: JSONDecodeError
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
            workflow = Workflow(name=data["name"])
            del data["name"]
            attrs = {k: data[k] for k in data if k != "name" and k != "tasks"}
            workflow.__dict__.update(attrs)
            for d in data["tasks"]:
                new_task = Task(operator=d["operator"], name=d["name"],
                                arguments={a.split("=")[0]: a.split("=")[1] for a in d["arguments"]})
                new_task.__dict__.update({k: d[k] for k in d if k != "name" and k != "operator" and k != "arguments"})
                workflow.addTask(new_task)
            return workflow

        data = file_check(file)
        check_workflow_name(data)
        workflow = start_workflow(data)
        return workflow


class Task:
    """Task(operator, arguments={}, name=None, type=None, **kwargs)
    Attributes:
        operator: Ophidia operator name
        arguments: list of user-defined operator arguments as key=value pairs
        name: unique task name
        type: type of the task

    Keyword Args:
        run: enable submission to analytics framework, yes or no
        on_exit: operation to be executed on output objects
        on_error: behaviour in case of error

    """
    attributes = ["run", "on_exit", "on_error"]
    active_attributes = ["name", "operator", "arguments"]

    def __init__(self, operator, arguments={}, name=None, type=None, **kwargs):
        """Task(operator, arguments={}, name=None, type=None, **kwargs)
        :param operator: Ophidia operator name
        :type operator: <class 'str'>
        :param arguments: list of user-defined operator arguments as key=value pairs
        :type arguments: <class 'dict'>
        :param name: unique task name
        :type name: <class 'str'>
        :param type: type of the task
        :type type: <class 'str'>
        :key run: enable submission to analytics framework, yes or no
        :type run: <class 'str'>
        :key on_exit: operation to be executed on output objects
        :type on_exit: <class 'str'>
        :key on_error: behaviour in case of error
        :type on_error: <class 'str'>
        """
        for k in kwargs.keys():
            if k not in self.attributes:
                raise AttributeError("Unknown Task argument: {0}".format(k))
        self.type = type if type else "ophidia"
        self.name = name
        self.operator = operator
        self.arguments = ["{0}={1}".format(k, arguments[k]) for k in arguments.keys()]
        self.dependencies = []
        self.__dict__.update(kwargs)

    def deinit(self):
        """deinit() -> None : reverse the initialization of the object
        :return: None
        :rtype: None
        """
        for k in self.active_attributes:
            self.__delattr__(k)

    def addDependency(self, task, argument=None):
        """addDependency(task, argument=None) -> None : adds dependencies to a task

        :param task: task name the current argument depends on
        :type task: <class 'pav.Task'>
        :param argument: argument depending on the output of the task 'task'
        :type argument: <class 'str'>
        :return: None
        """
        dependency_dict = {}
        if not argument:
            dependency_dict["type"] = "embedded"
        else:
            dependency_dict["argument"] = argument
            dependency_dict["type"] = "all"
        dependency_dict["task"] = task.__dict__["name"]
        self.dependencies.append(dependency_dict)

    def copyDependency(self, dependency):
        """copyDependency(dependency) -> None : copy a dependency instead of using addDependency, when it has the proper
        format
        :param dependency: dependency difct
        :type dependency: <class 'dict'>
        :return: None
        """
        self.dependencies.append(dependency)

    def reverted_arguments(self):
        """reverted_arguments() -> dict : changes the format of the arguments
        :return: <class 'dict'>
        """
        arguments = {}
        for arg in self.arguments:
            arguments[arg.split("=")[0]] = arg.split("=")[1]
        return arguments