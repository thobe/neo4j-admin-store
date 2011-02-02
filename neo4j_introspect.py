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
    """get all relationships linked to a node"""
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

@util
def scan(store, *filters):
    """Scan a store for records that match all supplied filters"""
    get = store.forceGetRecord
    for i in xrange(store.highId):
        item = get(i)
        for f in filters:
            if not f(item): break
        else:
            yield item

@util
def filter(**predicate):
    """Return true if any of the supplied properties have the supplied value"""
    def filter(item):
        for key, value in predicate.items():
            if getattr(item, key, None) == value: return True
        return False
    return filter

del util

def __main__(path=".",script=None):
    if not script:
        import os
        if os.path.isfile(path):
            path, script = ".", path
    env = dict(__utils)
    store = GraphDatabaseStore(path)
    env['store'] = store
    if script:
        with open(script) as script:
            store.makeStoreOk()
            try:
                exec(script.read(), env)
            finally:
                store.shutdown()
    else:
        from code import InteractiveConsole
        console = InteractiveConsole(locals=env)
        store.makeStoreOk()
        try:
            console.interact(
                banner="Neo4j Store introspection console (Python)")
        finally:
            store.shutdown()

if __name__ == '__main__':
    import sys
    try:
        __main__(*sys.argv[1:])
    except:
        import traceback; traceback.print_exc()
        print "USAGE:", sys.argv[0], "[<graphdb path>]"
