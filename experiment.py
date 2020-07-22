class Workflow:
    attributes = ["url", "sessionid", "exec_mode", "ncores", "nhost", "on_error", "on_exit", "run", "cwd", "cdd",
                  "cube", "callback_url", "output_format", "host_partition", "nthreads"]
    active_attributes = ["name", "author", "abstract"]
    task_attributes = ["run", "on_exit", "on_error"]
    task_name_counter = 1

    def __init__(self, name, author=None, abstract=None, **kwargs):
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
        for k in self.active_attributes:
            self.__delattr__(k)

    def addTask(self, task):
        if "name" not in task.__dict__.keys() or task.name is None:
            task.name = self.name + "_{0}".format(self.task_name_counter)
        if task.__dict__["name"] in [t.__dict__["name"] for t in self.tasks]:
            raise AttributeError("task already exists")
        if task.__dict__["dependencies"]:
            for dependency in task.__dict__["dependencies"]:
                if dependency["task"] not in [task.__dict__["name"] for task in self.tasks]:
                    raise AttributeError("dependency not fulfilled")
        self.task_name_counter += 1
        # self.tasks.append(task.__dict__)
        self.tasks.append(task)

    def getTask(self, taskname):
        tasks = [t for t in self.tasks if t["name"] == taskname]
        if len(tasks) == 1:
            return tasks[0]
        elif len(tasks) == 0:
            return None

    def save(self, workflowname):
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

    def set_name(self, name):
        self.name = name

    def newSubWorkflow(self, workflow, params, dependency=[], name=None):
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

        def add_dependencies(task, new_task, task_names_dict):
            if len(task.dependencies) > 0:
                for d in task.dependencies:
                    if d["task"] in task_names_dict:
                        d["task"] = task_names_dict[d["task"]]
                    new_task.copyDependency(d)

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
        old_task_names_new_task_names = {}
        for task in workflow.tasks:
            new_task_name = add_task_name(name, task.name, task_id)
            old_task_names_new_task_names[task.name] = new_task_name
            task_id += 1
            new_arguments = check_replace_args(params, task.reverted_arguments())
            new_task = Task(operator=task.operator, arguments=new_arguments, name=new_task_name)
            find_root_tasks_add_dependencies(task, dependency, new_task)
            add_dependencies(task, new_task, old_task_names_new_task_names)
            all_tasks.append(new_task)
            self.addTask(new_task)
            non_leaf_tasks += [t["task"] for t in task.dependencies]
        return [t for t in all_tasks if t.name not in non_leaf_tasks]

    @staticmethod
    def load(file):
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
    attributes = ["run", "on_exit", "on_error"]
    active_attributes = ["name", "operator", "arguments"]

    def __init__(self, operator, arguments={}, name=None, type=None, **kwargs):
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
        for k in self.active_attributes:
            self.__delattr__(k)

    def addDependency(self, task, argument=None):
        dependency_dict = {}
        if not argument:
            dependency_dict["type"] = "embedded"
        else:
            dependency_dict["argument"] = argument
            dependency_dict["type"] = "all"
        dependency_dict["task"] = task.__dict__["name"]
        self.dependencies.append(dependency_dict)

    def copyDependency(self, dependency):
        self.dependencies.append(dependency)

    def setName(self, name):
        self.name = name

    def reverted_arguments(self):
        arguments = {}
        for arg in self.arguments:
            arguments[arg.split("=")[0]] = arg.split("=")[1]
        return arguments