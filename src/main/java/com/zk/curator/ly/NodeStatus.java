package com.zk.curator.ly;

/**
 * ClassName: NodeStatus
 * Description:
 * Date: 2015/7/16 11:24
 *
 * @author gjf11847
 * @version V1.0
 * @since JDK 1.7
 */
public enum NodeStatus
{
    UNKNOWN("UNKNOWN"), ALIVE("ALIVE"), SUSPEND("SUSPEND"),
    RECOVERING("RECOVERING"), RECOVERED("RECOVERED"), DEAD("DEAD"),
    CRASH("CRASH");

    private String code;
    NodeStatus(String code)
    {
        this.code = code;
    }

    public String getCode()
    {
        return this.code;
    }
}
