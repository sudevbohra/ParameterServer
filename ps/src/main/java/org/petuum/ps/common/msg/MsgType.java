package org.petuum.ps.common.msg;

/**
 * Created by aqiao on 3/27/15.
 */
public class MsgType {
    public static final int CLIENT_CONNECT = 0;
    public static final int SERVER_CONNECT = 1;
    public static final int APP_CONNECT = 2;
    public static final int BG_CREATE_TABLE = 3;
    public static final int CREATE_TABLE = 4;
    public static final int CREATE_TABLE_REPLY = 5;
    public static final int CREATED_ALL_TABLES = 6;
    public static final int ROW_REQUEST = 7;
    public static final int ROW_REQUEST_REPLY = 8;
    public static final int SERVER_ROW_REQUEST_REPLY = 9;
    public static final int BG_CLOCK = 10;
    public static final int BG_SEND_OP_LOG = 11;
    public static final int CLIENT_SEND_OP_LOG = 12;
    public static final int CONNECT_SERVER = 13;
    public static final int CLIENT_START = 14;
    public static final int APP_THREAD_DEREG = 15;
    public static final int CLIENT_SHUT_DOWN = 16;
    public static final int SERVER_SHUT_DOWN_ACK = 17;
    public static final int SERVER_PUSH_ROW = 18;
    public static final int SERVER_OP_LOG_ACK = 19;
    public static final int BG_HANDLE_APPEND_OP_LOG = 20;
    public static final int MEM_TRANSFER = 50;
}
