import lib.connector
import lib.table
import lib.logging
import os
from os import walk
from multiprocessing import pool, Manager
from datetime import datetime
from blockdiag import parser, builder, drawer

# Default. Change in setup if You wish.
poolsize = 5
drawdiag = True

manager = Manager()
counters = manager.dict()
counters['inserts'] = 0
counters['updates'] = 0
counters['warnings'] = 0
counters['errors'] = 0

paths = manager.dict()

# masterlogger = logging.getLogger('master_logger')
# masterlogger.basicConfig(
#     format='%(asctime)s %(message)s'
#     , filename=os.path.dirname('./log/' + datetime.now().strftime('master_%H_%M_%d_%m_%Y.log')
#     , level=logging.WARNING)

#print(os.path.dirname('./log/' + datetime.now().strftime('master_%H_%M_%d_%m_%Y.log')))

masterlogger = lib.logging.Logger(
    name = 'Master'
    , filename='./Logs/' + datetime.now().strftime('master_%H_%M_%d_%m_%Y.log')
    , minlevel = 'ERROR')

def exec_flow((filename, flow_dir, counters)):
    with open('./flow/' + flow_dir + '/' + filename) as source_file:
        logpath = './flow/' + flow_dir + '/Logs/' + filename + datetime.now().strftime('_%H_%M_%d_%m_%Y.log')
        tables = []
        default_args = (logpath, masterlogger, counters, paths, tables)
        print('Executing ' + filename)
        exec(source_file.read())
    for table in tables:
        try:
            table.finalize()
        except:
            pass

setup_files = []
for (dirpath, dirnames, filenames) in walk('./setup'):
    setup_files.extend(filenames)
    break

for f in setup_files:
    if f[0] != '.':
        with open('./setup/' + f) as source_file:
            exec(source_file.read())

flow_dirs = []
for (dirpath, dirnames, filenames) in walk('./flow'):
    flow_dirs.extend(dirnames)
    break

for flow_dir in flow_dirs:
    flow_files = []
    for (dirpath, dirnames, filenames) in walk('./flow/' + flow_dir):
        flow_files.extend(filenames)
        break

    args = [(x, flow_dir, counters) for x in flow_files if x[0] != '.']
    # Parallel
    try:
        workerpool = pool.Pool(poolsize)
        workerpool.map(exec_flow, args)
    finally:
        workerpool.close()
        workerpool.join()

final_files = []
for (dirpath, dirnames, filenames) in walk('./finalization'):
    final_files.extend(filenames)
    break

for f in final_files:
    if f[0] != '.':
        with open('./finalization/' + f) as source_file:
            exec(source_file.read())

# drawing diagram
if drawdiag:
    dirs = dict()
    bdiagsrc = 'blockdiag {\n'
    for table in paths.keys():
        files = dict()
        d = paths[table][0]
        f = paths[table][1]
        if f not in files.keys():
            files[f] = [table]
        else:
            files[f].append(table)
        if d not in dirs.keys():
            dirs[d] = files
        else:
            if dirs[d] is None:
                dirs[d] = files
            else:
                if f not in dirs[d].keys():
                    dirs[d].update(files)
                else:
                    dirs[d][f] = dirs[d][f] + files[f]

    #print dirs
    #print paths
    #print files
    prevdir = None
    for d in sorted(dirs.keys()):
        bdiagsrc += d + ' [label = "' + d + '", shape = square];\n'
        dend = d + '_END'
        bdiagsrc += dend + ' [label = "", shape = minidiamond];'
        if prevdir is not None:
            bdiagsrc += prevdir + ' -> ' + d + ';'
        prevdir = dend
        for filename in sorted(dirs[d].keys()):
            bdiagsrc += filename + ' [label = "' + filename + ' - ' + ';'.join(str(t) for t in dirs[d][filename]) + '", shape = roundedbox];\n'
            bdiagsrc +=  d + ' -> ' + filename + ' -> ' + dend + ';\n'
            # bdiagsrc +=  d + ' -> ' + filename + ';\n'
            # prevtable = filename
            # for table in sorted(dirs[d][filename]):
            #     bdiagsrc += table + ' [label = "' + table + '"];\n'
            #     bdiagsrc +=  prevtable + ' -> ' + table + ';\n'
            #     prevtable = table
            # bdiagsrc += prevtable + ' -> ' + dend + ';\n'
    bdiagsrc += '}'
    tree = parser.parse_string(bdiagsrc)
    diagram = builder.ScreenNodeBuilder.build(tree)
    draw = drawer.DiagramDraw('PNG', diagram, filename="diagram.png")
    draw.draw()
    draw.save()

masterlogger.finalize()

print('Finished!')
if counters['inserts'] > 0:
    print(str(counters['inserts']) + ' inserts')
if counters['updates'] > 0:
    print(str(counters['updates']) + ' updates')
if counters['warnings'] > 0:
    print(str(counters['warnings']) + ' warnings')
if counters['errors'] > 0:
    print(str(counters['errors']) + ' errors')
