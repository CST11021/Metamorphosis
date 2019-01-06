package com.taobao.metamorphosis.cluster.json;

import java.io.Serializable;

import com.taobao.metamorphosis.utils.JSONUtils;


/**
 * Topic's broker info to be registed in zookeeper.
 * ��ʾtopicע����zk�ϵ���Ϣ�����磺{"numParts":1,"broker":"0-m"}
 * numParts��ʾtopic�Ŀ��÷�������
 * broker�е�0��ʾbrokeId,m��ʾmaster�����s��ʾ��salve
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

    /** ���Ӧ����broker��Ĭ�ϵķ����� */
    private int numParts;

    /** ��ʾbrokerId */
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
     * ��json���ݽ���Ϊ TopicBroker �������磺{"numParts":1,"broker":"0-m"}
     * @param json
     * @return
     * @throws Exception
     */
    public static TopicBroker parse(String json) throws Exception {
        return (TopicBroker) JSONUtils.deserializeObject(json, TopicBroker.class);
    }

    /**
     * �� TopicBroker תΪJSON
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
