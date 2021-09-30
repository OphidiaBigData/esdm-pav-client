class Workflow:
    pyophidia_client = None

    def __init__(self, experiment_id=None, workflow_object=None):
        if experiment_id:
            if workflow_object is not None:
                raise AttributeError("You can't provide both experiment_id and"
                                     "workflow_object")
            self.experiment_id = experiment_id
            self.workflow_object = None
        else:
            if experiment_id is not None:
                raise AttributeError("You can't provide both experiment_id and"
                                     "workflow_object")
            self.workflow_object = workflow_object
            self.experiment_id = None

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
        if self.experiment_id is None:
            raise AttributeError("Cancel requires workflow_id")
        self.__runtime_connect()
        self.pyophidia_client.submit(
            query="oph_cancel id={0};exec_mode=async;".format(self.experiment_id)
        )

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
        if self.experiment_id is not None:
            raise AttributeError("You can't submit a workflow that was already"
                                 "submitted")
        self.exec_mode = "async"
        dict_workflow = json.dumps(self.workflow_to_json())
        str_workflow = str(dict_workflow)
        self.__runtime_connect()
        self.pyophidia_client.wsubmit(str_workflow, *args)
        self.exec_mode = "sync"
        self.workflow_id = self.pyophidia_client.last_jobid.split("?")[1].split(
            "#"
        )[0]
        return self.workflow_id

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
                json.dumps(self.workflow_to_json())
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
            # dot = graphviz.Digraph(comment=self.workflow_object.name)
            dot = graphviz.Digraph(comment="sample_name")

            for task in tasks:
                dot.attr(
                    "node",
                    shape="circle",
                    width="1",
                    penwidth="1",
                    style="",
                    fontsize="10pt",
                )
                if len(task_dict.keys()) == 0:
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
            "(?i).*RUNNING$": "orange",
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
        # _check_workflow_validity()
        self.__runtime_connect()
        self.pyophidia_client.submit(
            "oph_resume id={0};".format(self.experiment_id)
        )
        status_response = json.loads(self.pyophidia_client.last_response)
        self.pyophidia_client.submit(
            "oph_resume document_type=request;level=3;id={0};".format(self.experiment_id)
        )
        json_response = json.loads(self.pyophidia_client.last_response)

        task_dict = {}
        tasks = _modify_task(json_response)
        sorted_tasks = _sort_tasks(tasks)
        print("status_response")
        print(status_response)
        workflow_status = _check_workflow_status(status_response)
        if iterative is True:
            while True:
                if visual_mode is True:
                    _draw(sorted_tasks, status_response, status_color_dictionary)
                else:
                    print(workflow_status)
                print("workflow status")
                print(workflow_status)

                if (not re.match("(?i).*RUNNING", workflow_status)
                    and
                    (not re.match("(?i).*PENDING", workflow_status))

                ):
                    return workflow_status
                time.sleep(frequency)
                self.pyophidia_client.submit(
                    "oph_resume id={0};".format(self.experiment_id)
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

    def workflow_to_json(self):
        non_workflow_fields = [
            "pyophidia_client",
            "task_name_counter",
            "workflow_id",
        ]

        new_workflow = {
            k: dict(self.workflow_object.__dict__)[k]
            for k in dict(self.workflow_object.__dict__).keys()
            if k not in non_workflow_fields
        }
        if "tasks" in new_workflow.keys():
            new_workflow["tasks"] = [t.__dict__ for t in new_workflow["tasks"]]
        return new_workflow

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
