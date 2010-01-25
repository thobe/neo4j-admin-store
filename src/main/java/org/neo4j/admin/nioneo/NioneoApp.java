package org.neo4j.admin.nioneo;

import org.neo4j.shell.impl.AbstractApp;

public abstract class NioneoApp extends AbstractApp
{
    public NioneoServer getServer()
    {
        return (NioneoServer) super.getServer();
    }
}
