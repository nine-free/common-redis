package com.soft1010.common.redis;

import redis.clients.jedis.Pipeline;

import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * @author wuchougui@le.com
 * @Create time: 2016/10/20
 * @Description: redis-Service
 */
public interface RedisService {

    Boolean exists(String key);

    String get(String key);

    /**
     * get的指操作
     * 批量从redis读取key/value
     *
     * @param keys
     * @return
     */
    Map<String, String> getAll(String[] keys);

    String put(String key, String value);

    /**
     * @param key        key
     * @param value      值
     * @param expireTime 失效时间（毫秒）
     */
    String put(String key, String value, long expireTime);

    /**
     * put的指操作
     * 批量将key/value写入redis
     *
     * @param keyValues
     */
    void putAll(Map<String, String> keyValues);

    /**
     * put的指操作
     * 批量将key/value写入redis
     * @param keyValues
     * @param expireTime 失效时间（毫秒）
     */
    void putAll(Map<String, String> keyValues, long expireTime);

    String hget(String key, String field);

    Long hset(String key, String field, String value);

    Long hset(byte[] key, byte[] field, byte[] value);

    String hmset(String key, Map<String, String> map);

    /**
     * @param key        key
     * @param map        值
     * @param expireTime 失效时间（毫秒）
     */
    Long hmset(String key, Map<String, String> map, long expireTime);

    Map<String, String> hgetAll(String key);

    Map<byte[], byte[]> hgetAll(byte[] key);

    Long increment(String key, String field);

    Long increment(String key, String field, long value);

    Long delete(String key, String field);

    Long delete(byte[] key, byte[] field);

    Long delete(String key, String... fields);

    Long delete(String key);

    Long delete(String... keys);

    Set<String> keys(String pattern);

    public Long putSset(String key, String productChannel);

    public Set<String> getSmembers(String key);

    /**
     * 将一个或多个值 value 插入到列表 key 的表尾(最右边)。
     *
     * @param key
     * @param values
     * @return
     */
    Long rpush(String key, String... values);

    /**
     * 移除并返回列表 key 的头元素。
     *
     * @param key
     * @return
     */
    String lpop(String key);

    Object pipeline(BiFunction<? super Pipeline, ? super Object[], ? super Object> call, Object... args);

    /**
     * 设置一个key的过期时间（单位：秒）
     *
     * @param key     key值
     * @param seconds 多少秒后过期
     * @return 1：设置了过期时间 0：没有设置过期时间/不能设置过期时间
     */
    long expire(String key, int seconds);
}
