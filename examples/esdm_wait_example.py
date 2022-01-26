from esdm_pav_client import Workflow, Experiment

e1 = Experiment(name="Example of wait",
            author="ESiWACE2",
            abstract="Wait exec example",
            exec_mode="async"
)

t1 = e1.newTask(name="Check ESDM data",
               type="control",
               operator='wait',
               arguments={"type": "file", 
                          "output": "esdm://tos", 
                          "measure": "tos", 
                          "subset_dims": "time", 
                          "subset_filter": "2002", 
                          "subset_type": "coord", 
                          "timeout": "10"})
t2 = e1.newTask(name="In-flight Avg",
               type="ophidia",
               operator='oph_importesdm',
               arguments={'measure': 'tos', 
                          'operation': 'avg',
                          'ioserver': 'ophidiaio_memory', 
                          'nfrag': '1'},
               dependencies={t1:'input'})
t3 = e1.newTask(name="Aggregate",
               type="ophidia",
               operator='oph_aggregate',
               arguments={'operation': 'avg'},
               dependencies={t2:'cube'})
t4 = e1.newTask(name="Export",
               type="ophidia",
               operator='oph_exportesdm',
               arguments={'output': 'esdm://tos_avg'},
               dependencies={t3:'cube'})

w1 = Workflow(e1)
w1.submit()
w1.monitor(frequency=1, iterative=True, visual_mode=True)

