from esdm_pav_client import Workflow, Experiment

e1 = Experiment(
    name="Example of parallel branches",
    author="ESiWACE2",
    abstract="Parallel exec example",
    exec_mode="async"
)

t1 = e1.newTask(name="Start loop",
               type="control",
               operator='for',
                arguments={"key": "index", "values": "$1", "parallel": "yes"})
t2 = e1.newTask(name="Regrid",
               type="cdo",
               operator='-remapbil,r90x45',
               arguments={'input': 'tasmax_input_@{index}.nc', 'output': 'esdm://tasmax_@{index}', 'force': 'yes'},
               dependencies={t1:''})
t3 = e1.newTask(name="Import",
               type="ophidia",
               operator='oph_importesdm',
               arguments={'measure': 'tasmax', 'imp_dim': 'time', 'ioserver': 'ophidiaio_memory'},
               dependencies={t2:'input'})
t4 = e1.newTask(name="Reduce",
               type="ophidia",
               operator='oph_reduce', 
               arguments={'operation': 'avg'},
               dependencies={t3:'cube'})
t5 = e1.newTask(name="End loop",
               type="control",
               operator='endfor',
               arguments={},
               dependencies={t4:'cube'})
t6 = e1.newTask(name="Merge",
               type="ophidia",
               operator='oph_mergecubes2', 
               arguments={"dim": "new_dim"}, 
               dependencies={t5:'cubes'})
t7 = e1.newTask(name="Export",
               type="ophidia",
               operator='oph_exportnc', 
               arguments={'output': 'tasmax_output.nc'},
               dependencies={t6:'cube'})

w1 = Workflow(e1)
w1.submit("2000|2001|2002|2003|2004|2005")
w1.monitor(frequency=1, iterative=True, visual_mode=True)
