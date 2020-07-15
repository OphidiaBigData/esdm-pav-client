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

    def setName(self, name):
        self.name = name

    def reverted_arguments(self):
        arguments = {}
        for arg in self.arguments:
            arguments[arg.split("=")[0]] = arg.split("=")[1]
        return arguments



# w1 = Workflow(name="PTA", author="CMCC Foundation", abstract="Workflow for the analysis of precipitation trends related to different scenarios. ${1} is ncores; ${2} is the list of models (e.g. CMCC-CM|CMCC-CMS); ${3} is the scenario (e.g. rcp45 or rcp85); ${4} is the frequency (e.g. day or mon); ${5} is the percentile (e.g. 0.9); ${6} is the past time subset (e.g. 1976_2006); ${7} is the future time subset (e.g. 2071_2101); ${8} is the geographic subset (e.g. 30:45|0:40); ${9} is the grid of output map using the format r<lon>x<lat> (e.g. r360x180), i.e. a global regular lon/lat grid; ${10} import type (optional), set to '1' in case only subsetting data have to be imported (default); ${11} I/O server type (optional).")
# t1 = w1.newTask(operator='oph_createcontainer', arguments={'container': 'work', 'dim': 'lat|lon|time', 'dim_type': 'double|double|double', 'hierarchy': 'oph_base|oph_base|oph_time', 'base_time': '1976-01-01', 'calendar': 'standard', 'units': 'd'}, dependencies={})
# t2 = w1.newTask(operator='oph_createcontainer', arguments={'container': 'historical', 'dim': 'lat|lon|time', 'dim_type': 'double|double|double', 'hierarchy': 'oph_base|oph_base|oph_time', 'base_time': '1976-01-01', 'calendar': 'standard', 'units': 'd'}, dependencies={})
# t3 = w1.newTask(operator='oph_createcontainer', arguments={'container': 'scenario', 'dim': 'lat|lon|time', 'dim_type': 'double|double|double', 'hierarchy': 'oph_base|oph_base|oph_time', 'base_time': '2070-01-01', 'calendar': 'standard', 'units': 'd'}, dependencies={})
# t4 = w1.newTask(operator='oph_for', arguments={'key': 'model', 'values': '${2}', 'parallel': 'yes'}, dependencies={t1: None, t2: None, t3: None})
# t5 = w1.newTask(operator='oph_if', arguments={'condition': '${10}'}, dependencies={t4: None})

# w2 = Workflow(name="PTA", author="CMCC Foundation", abstract="Workflow for the analysis of precipitation trends related to different scenarios. ${1} is ncores; ${2} is the list of models (e.g. CMCC-CM|CMCC-CMS); ${3} is the scenario (e.g. rcp45 or rcp85); ${4} is the frequency (e.g. day or mon); ${5} is the percentile (e.g. 0.9); ${6} is the past time subset (e.g. 1976_2006); ${7} is the future time subset (e.g. 2071_2101); ${8} is the geographic subset (e.g. 30:45|0:40); ${9} is the grid of output map using the format r<lon>x<lat> (e.g. r360x180), i.e. a global regular lon/lat grid; ${10} import type (optional), set to '1' in case only subsetting data have to be imported (default); ${11} I/O server type (optional).")
# t5 = w2.newTask(operator='oph_if', arguments={'condition': '${10}'}, dependencies={t4: None})
# t6 = w2.newTask(operator='oph_importnc2', arguments={'container': '$container_value}', 'exp_dim': 'lat|lon', 'imp_dim': 'time', 'measure': 'pr', 'src_path': '/INDIGO/precip_trend_data/@{model}/$path1/${4}/$cube_file', 'compressed': 'no', 'exp_concept_level': 'c|c', 'filesystem': 'local', 'imp_concept_level': '${4}', 'ndb': '1', 'ndbms': '1', 'nhost': '1', 'import_metadata': 'yes', 'check_compliance': 'no', 'units': 'd', 'subset_dims': 'time|lat|lon', 'subset_filter': '${6}|${8}', 'subset_type': 'coord', 'offset': '0|2|2', 'ioserver': '${11}'}, dependencies={t5: None})
# values_to_replace = {"$container_value": "scenario", "$path1": "${3}", "$cube_file": "pr_${4}_@{model}_${3}_r1i1p1.nc"}
# values_to_replace_2 = {"$container_value": "historical", "$path1": "historical", "$cube_file": "pr_${4}_@{model}_historical_r1i1p1.nc"}
#
# t15 = w1.newTask(operator='oph_importnc2', arguments={'container': 'scenario', 'exp_dim': 'lat|lon', 'imp_dim': 'time', 'measure': 'pr', 'src_path': '/INDIGO/precip_trend_data/@{model}/${3}/${4}/pr_${4}_@{model}_${3}_r1i1p1.nc', 'compressed': 'no', 'exp_concept_level': 'c|c', 'filesystem': 'local', 'imp_concept_level': '${4}', 'ndb': '1', 'ndbms': '1', 'nhost': '1', 'import_metadata': 'yes', 'check_compliance': 'no', 'units': 'd', 'subset_dims': 'time|lat|lon', 'subset_filter': '${7}|${8}', 'subset_type': 'coord', 'offset': '0|2|2', 'ioserver': '${11}'}, dependencies={t14: None})
# t6 = w1.newTask(operator='oph_importnc2', arguments={'container': 'historical', 'exp_dim': 'lat|lon', 'imp_dim': 'time', 'measure': 'pr', 'src_path': '/INDIGO/precip_trend_data/@{model}/historical/${4}/pr_${4}_@{model}_historical_r1i1p1.nc', 'compressed': 'no', 'exp_concept_level': 'c|c', 'filesystem': 'local', 'imp_concept_level': '${4}', 'ndb': '1', 'ndbms': '1', 'nhost': '1', 'import_metadata': 'yes', 'check_compliance': 'no', 'units': 'd', 'subset_dims': 'time|lat|lon', 'subset_filter': '${6}|${8}', 'subset_type': 'coord', 'offset': '0|2|2', 'ioserver': '${11}'}, dependencies={t5: None})
# t7 = w1.newTask(operator='oph_else', arguments={}, dependencies={t5: None})
# t8 = w1.newTask(operator='oph_importnc2', arguments={'container': 'historical', 'exp_dim': 'lat|lon', 'imp_dim': 'time', 'measure': 'pr', 'src_path': '/INDIGO/precip_trend_data/@{model}/historical/${4}/pr_${4}_@{model}_historical_r1i1p1.nc', 'compressed': 'no', 'exp_concept_level': 'c|c', 'filesystem': 'local', 'imp_concept_level': '${4}', 'ndb': '1', 'ndbms': '1', 'nhost': '1', 'import_metadata': 'yes', 'check_compliance': 'no', 'units': 'd', 'ioserver': '${11}'}, dependencies={t7: None})
# t9 = w1.newTask(operator='oph_subset', arguments={'subset_dims': 'time|lat|lon', 'subset_filter': '${6}|${8}', 'offset': '0|2|2'}, dependencies={t8:'cube'})
# t10 = w1.newTask(operator='oph_endif', arguments={}, dependencies={t6:'cube', t9:'cube'})
# t11 = w1.newTask(operator='oph_subset', arguments={'subset_dims': 'time', 'subset_filter': 'JJA', 'subset_type': 'coord', 'nthreads': '5'}, dependencies={t10:'cube'})
# t12 = w1.newTask(operator='oph_reduce2', arguments={'operation': 'quantile', 'dim': 'time', 'concept_level': 'y', 'order': '${5}', 'nthreads': '5'}, dependencies={t11:'cube'})
# t13 = w1.newTask(operator='oph_apply', arguments={'query': 'oph_gsl_fit_linear_coeff(measure)', 'measure_type': 'auto', 'nthreads': '5'}, dependencies={t12:'cube'})
#
# t16 = w1.newTask(operator='oph_else', arguments={}, dependencies={t14: None})
# t17 = w1.newTask(operator='oph_importnc2', arguments={'container': 'scenario', 'exp_dim': 'lat|lon', 'imp_dim': 'time', 'measure': 'pr', 'src_path': '/INDIGO/precip_trend_data/@{model}/${3}/${4}/pr_${4}_@{model}_${3}_r1i1p1.nc', 'compressed': 'no', 'exp_concept_level': 'c|c', 'filesystem': 'local', 'imp_concept_level': '${4}', 'ndb': '1', 'ndbms': '1', 'nhost': '1', 'import_metadata': 'yes', 'check_compliance': 'no', 'units': 'd', 'ioserver': '${11}'}, dependencies={t16: None})
# t18 = w1.newTask(operator='oph_subset', arguments={'subset_dims': 'time|lat|lon', 'subset_filter': '${7}|${8}', 'offset': '0|2|2'}, dependencies={t17:'cube'})
# t19 = w1.newTask(operator='oph_endif', arguments={}, dependencies={t18:'cube',t15:'cube'})
# t20 = w1.newTask(operator='oph_subset', arguments={'subset_dims': 'time', 'subset_filter': 'JJA', 'subset_type': 'coord', 'nthreads': '5'}, dependencies={t19:'cube'})
# t21 = w1.newTask(operator='oph_reduce2', arguments={'operation': 'quantile', 'dim': 'time', 'concept_level': 'y', 'order': '${5}', 'nthreads': '5'}, dependencies={t20:'cube'})
# t22 = w1.newTask(operator='oph_apply', arguments={'query': 'oph_gsl_fit_linear_coeff(measure)', 'measure_type': 'auto', 'nthreads': '5'}, dependencies={t21:'cube'})
#
#
# task_array = w1.newSubWorkflow(name="new_subworkflow", workflow=w2, params={"$args1": "value1", "args2": "value2"})

# w1 = Workflow.load(file="sample_addTask.json")
# w1.save("reverse_sample_addTask.json")