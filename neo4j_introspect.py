#!/bin/sh
# -*- mode: Python; coding: utf-8 -*-
""":"
PY=$0
while [ -L "$PY" ]; do
    PY=`readlink $PY`
done

pushd $(dirname $PY) > /dev/null

if ! mvn package dependency:copy-dependencies -DskipTests > /dev/null
then
    echo BUILD ERRORS
    exit $!
fi

export CLASSPATH=$(find `pwd` -name *.jar | tr "\\n" ":" | sed 's/:$//')

popd > /dev/null

jython $PY "$@"

"exit"""
from __future__ import with_statement

import sys

from org.neo4j.kernel.impl.nioneo.store import GraphDatabaseStore
from org.neo4j.admin.check import RecordInconsistency

def consistencyCheck(self):
    return RecordInconsistency.check(self)
GraphDatabaseStore.check = consistencyCheck

__utils = {}
def util(func):
    __utils[func.__name__] = func
    return func

@util
def relationships(store, node, rel=None,reverse=False):
    """get all relationships linked to a node"""
    if isinstance(node, int):
        node = store.nodeStore.forceGetRecord(node)
    _node = node.id
    if rel is None:
        _rel = node.nextRel
    elif isinstance(rel, int):
        _rel = rel
    else:
        _rel = rel.id
    while _rel != -1:
        rel = store.relStore.forceGetRecord(_rel)
        yield rel
        if rel.firstNode == _node:
            if reverse:
                _rel = rel.firstPrevRel
            else:
                _rel = rel.firstNextRel
        elif rel.secondNode == _node:
            if reverse:
                _rel = rel.secondPrevRel
            else:
                _rel = rel.secondNextRel
        else:
            raise ValueError

@util
def vizhood(store, start, file=None):
    def printRelationLink(label, relationId):
        if (relationId != -1):
          file.write('  %s -> %s [label=%s]\n'
                       % (rel.id, relationId, label))
    def node(nodeId):
        if nodeId not in nodes:
            file.write('  node%s [shape=ellipse; label=%s]\n' % (nodeId,nodeId))
            nodes.add(nodeId)
            node = store.nodeStore.forceGetRecord(nodeId)
            file.write('  node%s -> %s\n' % (nodeId,node.nextRel))
        return 'node%s' % nodeId
    close = False
    if file is None:
        file = sys.stdout
    elif isinstance(file, str):
        file = open(file,'w')
        close = True
    try:
        file.write('digraph NodeNeighborhood {\n')
        file.write('  node [shape=box]\n')
        nodes = set()
        for rel in relationships(store, start):
            file.write('  %s -> %s [label=first]\n'
                       % (rel.id, node(rel.firstNode)))
            file.write('  %s -> %s [label=second]\n'
                       % (rel.id, node(rel.secondNode)))
            printRelationLink('firstPrev', rel.firstPrevRel)
            printRelationLink('firstNext', rel.firstNextRel)
            printRelationLink('secondPrev', rel.secondPrevRel)
            printRelationLink('secondNext', rel.secondNextRel)
        file.write('}\n')
    finally:
        if close: file.close()

@util
def used(item):
    return item.inUse()

@util
def unused(item):
    return not item.inUse()

@util
def filter(**predicate):
    """Return true if any of the supplied properties have the supplied value"""
    def filter(item):
        for key, value in predicate.items():
            if getattr(item, key, None) == value: return True
        return False
    return filter

@util
def printAll(it, *attrs):  
    for el in it:
        if not attrs:
            print el
            continue
        print "; ".join(("%s: %s" % (attr,getattr(el,attr,None)))
                       for attr in attrs)

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
    try:
        __main__(*sys.argv[1:])
    except:
        import traceback; traceback.print_exc()
        print "USAGE:", sys.argv[0], "[<graphdb path>]"
