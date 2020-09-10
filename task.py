class Task:
    """
    Creates a Task object that will be used to be embedded on the workflow

    Construction::
    t1 = Task(name="Sample name", operator="oph_createcontainer", arguments=['container=work', 'dim=lat|lon|time',
        'dim_type=double|double|double', 'hierarchy=oph_base|oph_base|oph_time', 'base_time=1976-01-01',
        'calendar=standard', 'units=d'], on_error="skip")

    Parameters
    ----------
    operator : str
        Ophidia operator name
    arguments : dict
        list of user-defined operator arguments as key=value pairs
    name : str
        unique task name
    type : str
        type of the task
    run : str, optional
        enable submission to analytics framework, yes or no
    on_error : str, optional
        behaviour in case of error
    on_exit : str, optional
        operation to be executed on output objects
    """
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
        """
        Reverse the initialization of the object
        """
        for k in self.active_attributes:
            self.__delattr__(k)

    def addDependency(self, task, argument=None):
        """
        Adds previous tasks as a dependency on the current task

        Parameters
        ----------
        task : pav.Task
            task name the current argument depends on
        argument : str
            argument depending on the output of the task 'task'

        Raises
        ------
        AttributeError
            When one of the parameters has the wrong type

        Example
        -------
        t2 = w2.newTask(operator='oph_reduce', arguments={'operation': 'avg'})
        t3 = Task(name="Create Historical Container", operator="oph_createcontainer", arguments={}, on_error="skip")
        t3.addDependency(t2)
        """
        def parameter_check(task, argument):
            if not isinstance(argument, str):
                raise AttributeError("argument must be string")
            if not isinstance(task, Task):
                raise AttributeError("task must be Task object")

        parameter_check(task, argument)
        dependency_dict = {}
        if not argument:
            dependency_dict["type"] = "embedded"
        else:
            dependency_dict["argument"] = argument
            dependency_dict["type"] = "all"
        dependency_dict["task"] = task.__dict__["name"]
        self.dependencies.append(dependency_dict)

    def copyDependency(self, dependency):
        """
        Copy a dependency instead of using addDependency, when it has the proper format

        Parameters
        ----------
        dependency : dict
            Copy a dependency to a task
        """
        self.dependencies.append(dependency)

    def reverted_arguments(self):
        """
        Changes the format of the arguments

        Returns
        -------
        arguments : dict
            returns the arguments with the newest format
        """
        arguments = {}
        for arg in self.arguments:
            arguments[arg.split("=")[0]] = arg.split("=")[1]
        return arguments