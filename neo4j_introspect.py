#!/bin/sh
# -*- mode: Python; coding: utf-8 -*-
""":"
PY=$0
while [ -L "$PY" ]; do
    PY=`readlink $PY`
done

pushd $(dirname $PY) > /dev/null

if ! mvn package dependency:copy-dependencies -DskipTests=true > /dev/null
then
    echo BUILD ERRORS
    exit $!
fi

export CLASSPATH=$(find `pwd` -name *.jar | tr "\\n" ":" | sed 's/:$//')

popd > /dev/null

jython $PY "$@"

"exit"""
from __future__ import with_statement

from org.neo4j.kernel.impl.nioneo.store import GraphDatabaseStore

__utils = {}
def util(func):
    __utils[func.__name__] = func
    return func

@util
def relationships(store, node):
    if isinstance(node, int):
        node = store.nodeStore.forceGetRecord(node)
    _node = node.id
    _rel = node.nextRel
    while _rel != -1:
        rel = store.relStore.forceGetRecord(_rel)
        yield rel
        if rel.firstNode == _node:
            _rel = rel.firstNextRel
        elif rel.secondNode == _node:
            _rel = rel.secondNextRel
        else:
            raise ValueError

del util

def __main__(path=".",script=None):
    if not script:
        import os
        if os.path.isfile(path):
            path, script = ".", path
    env = dict(__utils)
    env['store'] = GraphDatabaseStore(path)
    if script:
        with open(script) as script:
            exec(script.read(), env)
    else:
        from code import InteractiveConsole
        console = InteractiveConsole(locals=env)
        console.interact(banner="Neo4j Store introspection console (Python)")

if __name__ == '__main__':
    import sys
    try:
        __main__(*sys.argv[1:])
    except:
        import traceback; traceback.print_exc()
        print "USAGE:", sys.argv[0], "[<graphdb path>]"
