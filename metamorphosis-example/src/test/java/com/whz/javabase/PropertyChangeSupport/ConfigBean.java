package com.whz.javabase.PropertyChangeSupport;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;

/**
 * @Author: wanghz
 * @Date: 2018/12/24 10:51 AM
 */
public class ConfigBean {

    private String configParam;

    private final PropertyChangeSupport propertyChangeSupport = new PropertyChangeSupport(this);

    public ConfigBean() {
        propertyChangeSupport.addPropertyChangeListener("configParam", new PropertyChangeListener() {

            @Override
            public void propertyChange(final PropertyChangeEvent evt) {
                System.out.println("属性变更了：" + evt.toString());
            }

        });
    }

    public String getConfigParam() {
        return configParam;
    }

    public void setConfigParam(String configParam) {
        this.configParam = configParam;
        this.propertyChangeSupport.firePropertyChange("configParam", null, null);
    }

    public PropertyChangeSupport getPropertyChangeSupport() {
        return propertyChangeSupport;
    }

    public static void main(String[] args) {
        ConfigBean configBean = new ConfigBean();
        configBean.setConfigParam("test1");
        configBean.setConfigParam("test2");
    }

}
