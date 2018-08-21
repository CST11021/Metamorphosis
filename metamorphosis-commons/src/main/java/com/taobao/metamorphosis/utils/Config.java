package com.taobao.metamorphosis.utils;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

/**
 * �����ļ�����ĳ����࣬MetaConfig��SlaveConfig��TopicConfig��ZKConfig ���̳��Ը���
 */
public abstract class Config {

    /**
     * ��ȡ�������Ա��������������������������Field����@Ignoreע�⣬�򲻷���
     * @return
     */
    public Set<String> getFieldSet() {
        Class<? extends Config> clazz = this.getClass();
        Field[] fields = clazz.getDeclaredFields();
        Set<String> rt = new HashSet<String>();
        for (Field f : fields) {
            String name = f.getName();
            // ����̳�Config���Field����@Ignoreע�⣬������
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
     * ����fields���������ַ���value�����Ƶ�һ��Ԫ��
     * @param fields
     * @param value
     * @return
     */
    public String findBestMatchField(Set<String> fields, String value) {
        int minScore = Integer.MAX_VALUE;
        String matchedField = null;
        for (String f : fields) {
            // ͳ��value��ÿ���ַ���f��ÿ���ַ���ͬ���ֵ��ַ�����
            int dis = StringUtils.getLevenshteinDistance(value, f);
            if (dis < minScore) {
                matchedField = f;
                minScore = dis;
            }
        }
        return matchedField;
    }

    /**
     * У�鼯��configKeySet�е�Ԫ���Ƿ���validKeySet�У���������ڣ����׳��쳣
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
