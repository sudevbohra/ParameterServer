package org.petuum.ps.common.network;

/**
 * Created by yihuaf on 2/23/15.
 */

public interface CommBus {

    public static final int K_NONE = 0;
    public static final int K_IN_PROC = 1;
    public static final int K_INTER_PROC = 2;

    public void close();

    public void threadRegister(Config config);

    public void threadDeregister();

    /**
     * Connect to a local thread Info is a customer-defined number to be
     * included in the Connect message, how to use it is up to the customer.
     *
     * @param entityId
     * @param connectMsg
     */
    public abstract void connectTo(int entityId, Msg connectMsg);

    /**
     * Connect to a remote thread.
     *
     * @param entityId
     * @param networkAddr
     * @param connectMsg
     */
    public void connectTo(int entityId, String networkAddr, Msg connectMsg);

    public boolean send(int entityId, Msg data);

    public boolean sendInproc(int entityId, Msg data);

    public boolean sendInterproc(int entityId, Msg data);

    public Msg recv();

    public Msg recv(long timeoutMilli);

    public Msg recvAsync();

    public boolean isLocalEntity(int entityId);

    public static abstract class WaitMsgFunc {
        public abstract Msg invoke();
    }

    public static abstract class WaitMsgTimeOutFunc {
        public abstract Msg invoke(long timeoutMilli);
    }

    public static class Config {
        /**
         * My thread id.
         */
        public int entityId;
        /**
         * What should I listen to?
         */
        public int lType;

        /**
         * In the format of "ip:port", such as "192.168.1.1:9999". It must be
         * set if ((ltype_ & kInterProc) == true)
         */
        public String networkAddr;

        public int numBytesInprocSendBuff;
        public int numBytesInprocRecvBuff;
        public int numBytesInterprocSendBuff;
        public int numBytesInterprocRecvBuff;

        public String ip;
        public int port;

        public Config() {
            this.entityId = 0;
            this.lType = CommBus.K_NONE;
            this.numBytesInprocSendBuff = 0;
            this.numBytesInprocRecvBuff = 0;
            this.numBytesInterprocSendBuff = 0;
            this.numBytesInterprocRecvBuff = 0;
        }

        public Config(int entityId, int lType, String networkAddr) {
            this.entityId = entityId;
            this.lType = lType;
            this.networkAddr = networkAddr;
            this.numBytesInprocSendBuff = 0;
            this.numBytesInprocRecvBuff = 0;
            this.numBytesInterprocSendBuff = 0;
            this.numBytesInterprocRecvBuff = 0;

        }

    }

}
