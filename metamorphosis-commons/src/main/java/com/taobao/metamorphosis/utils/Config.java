package com.taobao.metamorphosis.utils;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

/**
 * 配置文件对象的抽象父类，MetaConfig、SlaveConfig、TopicConfig和ZKConfig 都继承自该类
 */
public abstract class Config {

    /**
     * 获取所有属性变量名（即配置项），如果配置类的Field带有@Ignore注解，则不返回
     * @return
     */
    public Set<String> getFieldSet() {
        Class<? extends Config> clazz = this.getClass();
        Field[] fields = clazz.getDeclaredFields();
        Set<String> rt = new HashSet<String>();
        for (Field f : fields) {
            String name = f.getName();
            // 如果继承Config类的Field带有@Ignore注解，则跳过
            Ignore ignore = f.getAnnotation(Ignore.class);
            if (ignore != null) {
                continue;
            }

            Key key = f.getAnnotation(Key.class);
            if (key != null) {
                name = key.name();
                if (!StringUtils.isBlank(name)) {
                    rt.add(name);
                } else {
                    rt.add(f.getName());
                }
            }
            else if (name.length() > 0 && Character.isLowerCase(name.charAt(0))) {
                rt.add(name);
            }
        }
        return rt;
    }

    /**
     * 返回fields集合中与字符串value最相似的一个元素
     * @param fields
     * @param value
     * @return
     */
    public String findBestMatchField(Set<String> fields, String value) {
        int minScore = Integer.MAX_VALUE;
        String matchedField = null;
        for (String f : fields) {
            // 统计value中每个字符与f中每个字符不同部分的字符个数
            int dis = StringUtils.getLevenshteinDistance(value, f);
            if (dis < minScore) {
                matchedField = f;
                minScore = dis;
            }
        }
        return matchedField;
    }

    /**
     * 校验集合configKeySet中的元素是否都在validKeySet中，如果不存在，则抛出异常
     * @param configKeySet
     * @param validKeySet
     */
    public void checkConfigKeys(Set<String> configKeySet, Set<String> validKeySet) {
        for (String key : configKeySet) {
            if (!validKeySet.contains(key)) {
                String best = this.findBestMatchField(validKeySet, key);
                throw new IllegalArgumentException("Invalid config key:" + key + ",do you mean '" + best + "'?");
            }
        }
    }

}
