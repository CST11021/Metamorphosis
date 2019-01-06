package com.taobao.metamorphosis.cluster.json;

import java.io.Serializable;

import com.taobao.metamorphosis.utils.JSONUtils;


/**
 * Topic's broker info to be registed in zookeeper.
 * 表示topic注册在zk上的信息，例如：{"numParts":1,"broker":"0-m"}
 * numParts表示topic的可用分区数量
 * broker中的0表示brokeId,m表示master，如果s表示是salve
 * 
 * @author dennis
 * @since 1.4.3
 * @date 2012-05-19
 * 
 */
public class TopicBroker implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /** 这个应该是broker下默认的分区数 */
    private int numParts;

    /** 表示brokerId */
    private String broker;


    public TopicBroker() {
        super();
    }
    public TopicBroker(int numParts, String broker) {
        super();
        this.numParts = numParts;
        this.broker = broker;
    }


    /**
     * 将json数据解析为 TopicBroker 对象，例如：{"numParts":1,"broker":"0-m"}
     * @param json
     * @return
     * @throws Exception
     */
    public static TopicBroker parse(String json) throws Exception {
        return (TopicBroker) JSONUtils.deserializeObject(json, TopicBroker.class);
    }

    /**
     * 将 TopicBroker 转为JSON
     * @return
     * @throws Exception
     */
    public String toJson() throws Exception {
        return JSONUtils.serializeObject(this);
    }




    public int getNumParts() {
        return this.numParts;
    }
    public void setNumParts(int numParts) {
        this.numParts = numParts;
    }
    public String getBroker() {
        return this.broker;
    }
    public void setBroker(String broker) {
        this.broker = broker;
    }

    @Override
    public String toString() {
        try {
            return this.toJson();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.broker == null ? 0 : this.broker.hashCode());
        result = prime * result + this.numParts;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        TopicBroker other = (TopicBroker) obj;
        if (this.broker == null) {
            if (other.broker != null) {
                return false;
            }
        }
        else if (!this.broker.equals(other.broker)) {
            return false;
        }
        if (this.numParts != other.numParts) {
            return false;
        }
        return true;
    }
}
