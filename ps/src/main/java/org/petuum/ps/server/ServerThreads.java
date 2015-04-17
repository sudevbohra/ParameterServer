package org.petuum.ps.server;

public class ServerThreads {

    private static ServerThreadGroup serverThreadGroup;

    public static void init(int serverIdSt) {
        serverThreadGroup = new ServerThreadGroup(serverIdSt);
        serverThreadGroup.start();
    }

    public static void shutdown() {
        serverThreadGroup.shutdown();
    }

}
