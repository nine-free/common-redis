package com.soft1010.common.redis.impl;

import com.soft1010.common.redis.RedisService;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.util.Pool;

import java.util.*;
import java.util.function.BiFunction;

/**
 * @author wuchougui@le.com
 * @Create time: 2016/10/20
 * @Description: redis-Service实现
 */
public class RedisServiceImpl implements RedisService {

    private final Pool<Jedis> jedisPool;

    public RedisServiceImpl(Pool<Jedis> jedisPool) {
        this.jedisPool = jedisPool;
    }


    public Boolean exists(String key) {
        Jedis jedis = null;
        try {
            jedis = this.jedisPool.getResource();
            return jedis.exists(key);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    public String get(String key) {
        Jedis jedis = null;
        boolean oprSuccess = true;
        try {
            jedis = this.jedisPool.getResource();
            return jedis.get(key);
        } catch (Exception e) {
            oprSuccess = false;
            if (jedis != null)
                this.jedisPool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        } finally {
            if (oprSuccess)
                this.jedisPool.returnResource(jedis);

        }
    }

    public Map<String, String> getAll(String[] keys) {
        Jedis jedis = null;
        boolean oprSuccess = true;
        try {
            jedis = this.jedisPool.getResource();
            Pipeline p = jedis.pipelined();

            Map<String, Response<String>> responseMap = new HashMap<>();
            for (String key : keys) {
                responseMap.put(key, p.get(key));
            }

            p.sync();

            Map<String, String> map = new HashMap<>();
            for (Map.Entry<String, Response<String>> keyValuePair : responseMap.entrySet()) {
                map.put(keyValuePair.getKey(), keyValuePair.getValue().get());
            }

            return map;

        } catch (Exception e) {
            oprSuccess = false;
            if (jedis != null)
                this.jedisPool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        } finally {
            if (oprSuccess)
                this.jedisPool.returnResource(jedis);

        }
    }

    @Override
    public String put(String key, String value) {
        Jedis jedis = null;
        boolean oprSuccess = true;
        try {
            jedis = this.jedisPool.getResource();
            return jedis.set(key, value);
        } catch (Exception e) {
            oprSuccess = false;
            if (jedis != null)
                this.jedisPool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        } finally {
            if (oprSuccess)
                this.jedisPool.returnResource(jedis);
        }
    }

    @Override
    public String put(String key, String value, long expireTime) {
        Jedis jedis = null;
        boolean oprSuccess = true;
        try {
            jedis = this.jedisPool.getResource();
            return jedis.psetex(key, expireTime, value);
        } catch (Exception e) {
            oprSuccess = false;
            if (jedis != null)
                this.jedisPool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        } finally {
            if (oprSuccess)
                this.jedisPool.returnResource(jedis);
        }
    }

    public void putAll(Map<String, String> keyValues) {
        Jedis jedis = null;
        boolean oprSuccess = true;
        try {
            jedis = this.jedisPool.getResource();
            Pipeline p = jedis.pipelined();

            for (Map.Entry<String, String> keyValuePair : keyValues.entrySet()) {
                p.set(keyValuePair.getKey(), keyValuePair.getValue());
            }

            p.sync();
        } catch (Exception e) {
            oprSuccess = false;
            if (jedis != null)
                this.jedisPool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        } finally {
            if (oprSuccess)
                this.jedisPool.returnResource(jedis);
        }
    }

    public void putAll(Map<String, String> keyValues, long expireTime) {
        Jedis jedis = null;
        boolean oprSuccess = true;
        try {
            jedis = this.jedisPool.getResource();
            Pipeline p = jedis.pipelined();

            for (Map.Entry<String, String> keyValuePair : keyValues.entrySet()) {
                p.psetex(keyValuePair.getKey(), expireTime, keyValuePair.getValue());
            }

            p.sync();
        } catch (Exception e) {
            oprSuccess = false;
            if (jedis != null)
                this.jedisPool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        } finally {
            if (oprSuccess)
                this.jedisPool.returnResource(jedis);
        }
    }

    public String hget(String key, String field) {
        Jedis jedis = null;
        boolean oprSuccess = true;
        try {
            jedis = this.jedisPool.getResource();
            return jedis.hget(key, field);
        } catch (Exception e) {
            oprSuccess = false;
            if (jedis != null)
                this.jedisPool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        } finally {
            if (oprSuccess)
                this.jedisPool.returnResource(jedis);
        }
    }

    public Long hset(String key, String field, String value) {
        Jedis jedis = null;
        boolean oprSuccess = true;
        try {
            jedis = this.jedisPool.getResource();
            return jedis.hset(key, field, value);
        } catch (Exception e) {
            oprSuccess = false;
            if (jedis != null)
                this.jedisPool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        } finally {
            if (oprSuccess)
                this.jedisPool.returnResource(jedis);
        }
    }

    public Long hset(byte[] key, byte[] field, byte[] value) {
        Jedis jedis = null;
        boolean oprSuccess = true;
        try {
            jedis = this.jedisPool.getResource();
            return jedis.hset(key, field, value);
        } catch (Exception e) {
            oprSuccess = false;
            if (jedis != null)
                this.jedisPool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        } finally {
            if (oprSuccess)
                this.jedisPool.returnResource(jedis);
        }
    }

    public String hmset(String key, Map<String, String> map) {
        Jedis jedis = null;
        boolean oprSuccess = true;
        try {
            jedis = this.jedisPool.getResource();
            return jedis.hmset(key, map);
        } catch (Exception e) {
            oprSuccess = false;
            if (jedis != null)
                this.jedisPool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        } finally {
            if (oprSuccess)
                this.jedisPool.returnResource(jedis);
        }
    }

    @Override
    public Long hmset(String key, Map<String, String> map, long expireTime) {
        Jedis jedis = null;
        boolean oprSuccess = true;
        try {
            jedis = this.jedisPool.getResource();
            jedis.hmset(key, map);
            return jedis.pexpire(key, expireTime);
        } catch (Exception e) {
            oprSuccess = false;
            if (jedis != null)
                this.jedisPool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        } finally {
            if (oprSuccess)
                this.jedisPool.returnResource(jedis);
        }
    }

    public Map<String, String> hgetAll(String key) {
        Jedis jedis = null;
        boolean oprSuccess = true;
        try {
            jedis = this.jedisPool.getResource();
            return jedis.hgetAll(key);
        } catch (Exception e) {
            oprSuccess = false;
            if (jedis != null)
                this.jedisPool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        } finally {
            if (oprSuccess)
                this.jedisPool.returnResource(jedis);
        }
    }

    public Map<byte[], byte[]> hgetAll(byte[] key) {
        Jedis jedis = null;
        boolean oprSuccess = true;
        try {
            jedis = this.jedisPool.getResource();
            return jedis.hgetAll(key);
        } catch (Exception e) {
            oprSuccess = false;
            if (jedis != null)
                this.jedisPool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        } finally {
            if (oprSuccess)
                this.jedisPool.returnResource(jedis);
        }
    }

    public Long putSset(String key, String productChannel) {
        Jedis jedis = null;
        boolean oprSuccess = true;
        try {
            jedis = this.jedisPool.getResource();
            return jedis.sadd(key, productChannel);
        } catch (Exception e) {
            oprSuccess = false;
            if (jedis != null)
                this.jedisPool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        } finally {
            if (oprSuccess)
                this.jedisPool.returnResource(jedis);
        }
    }

    public Set<String> getSmembers(String key) {
        Jedis jedis = null;
        boolean oprSuccess = true;
        try {
            jedis = this.jedisPool.getResource();
            return jedis.smembers(key);
        } catch (Exception e) {
            oprSuccess = false;
            if (jedis != null)
                this.jedisPool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        } finally {
            if (oprSuccess)
                this.jedisPool.returnResource(jedis);
        }
    }

    public Long increment(String key, String field) {
        Jedis jedis = null;
        boolean oprSuccess = true;
        try {
            jedis = this.jedisPool.getResource();
            return jedis.hincrBy(key, field, 1);
        } catch (Exception e) {
            oprSuccess = false;
            if (jedis != null)
                this.jedisPool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        } finally {
            if (oprSuccess)
                this.jedisPool.returnResource(jedis);
        }
    }

    public Long increment(String key, String field, long value) {
        Jedis jedis = null;
        boolean oprSuccess = true;
        try {
            jedis = this.jedisPool.getResource();
            return jedis.hincrBy(key, field, value);
        } catch (Exception e) {
            oprSuccess = false;
            if (jedis != null)
                this.jedisPool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        } finally {
            if (oprSuccess)
                this.jedisPool.returnResource(jedis);
        }
    }

    public Long delete(String key, String field) {
        Jedis jedis = null;
        boolean oprSuccess = true;
        try {
            jedis = this.jedisPool.getResource();
            return jedis.hdel(key, field);
        } catch (Exception e) {
            oprSuccess = false;
            if (jedis != null)
                this.jedisPool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        } finally {
            if (oprSuccess)
                this.jedisPool.returnResource(jedis);
        }
    }

    public Long delete(byte[] key, byte[] field) {
        Jedis jedis = null;
        boolean oprSuccess = true;
        try {
            jedis = this.jedisPool.getResource();
            return jedis.hdel(key, field);
        } catch (Exception e) {
            oprSuccess = false;
            if (jedis != null)
                this.jedisPool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        } finally {
            if (oprSuccess)
                this.jedisPool.returnResource(jedis);
        }
    }

    public Long delete(String key, String... fields) {
        Jedis jedis = null;
        boolean oprSuccess = true;
        try {
            jedis = this.jedisPool.getResource();
            return jedis.hdel(key, fields);
        } catch (Exception e) {
            oprSuccess = false;
            if (jedis != null)
                this.jedisPool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        } finally {
            if (oprSuccess)
                this.jedisPool.returnResource(jedis);
        }
    }

    public Long delete(String key) {
        Jedis jedis = null;
        boolean oprSuccess = true;
        try {
            jedis = this.jedisPool.getResource();
            return jedis.del(key);
        } catch (Exception e) {
            oprSuccess = false;
            if (jedis != null)
                this.jedisPool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        } finally {
            if (oprSuccess)
                this.jedisPool.returnResource(jedis);
        }
    }

    public Long delete(String... keys) {
        Jedis jedis = null;
        boolean oprSuccess = true;
        try {
            jedis = this.jedisPool.getResource();
            return jedis.del(keys);
        } catch (Exception e) {
            oprSuccess = false;
            if (jedis != null)
                this.jedisPool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        } finally {
            if (oprSuccess)
                this.jedisPool.returnResource(jedis);
        }
    }

    public Set<String> keys(String pattern) {
        Jedis jedis = null;
        boolean oprSuccess = true;
        try {
            jedis = this.jedisPool.getResource();
            return jedis.keys(pattern);
        } catch (Exception e) {
            oprSuccess = false;
            if (jedis != null)
                this.jedisPool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        } finally {
            if (oprSuccess)
                this.jedisPool.returnResource(jedis);
        }
    }

    public List<Map> pipelined(Set<String> keySet) {
        List<Map> list = new ArrayList<Map>();
        Jedis jedis = null;
        boolean oprSuccess = true;
        try {
            jedis = this.jedisPool.getResource();
            List<Response<Map<String, String>>> responses = new ArrayList<Response<Map<String, String>>>();
            Pipeline pipeline = jedis.pipelined();
            for (String key : keySet) {
                responses.add(pipeline.hgetAll(key));
            }
            pipeline.sync();
            for (Response<Map<String, String>> response : responses) {
                list.add(response.get());
            }
            return list;
        } catch (Exception e) {
            oprSuccess = false;
            if (jedis != null)
                this.jedisPool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        } finally {
            if (oprSuccess)
                this.jedisPool.returnResource(jedis);
        }
    }

    public Long rpush(String key, String... values) {
        Jedis jedis = null;
        boolean oprSuccess = true;
        try {
            jedis = this.jedisPool.getResource();
            return jedis.rpush(key, values);
        } catch (Exception e) {
            oprSuccess = false;
            if (jedis != null)
                this.jedisPool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        } finally {
            if (oprSuccess)
                this.jedisPool.returnResource(jedis);

        }
    }

    public String lpop(String key) {
        Jedis jedis = null;
        boolean oprSuccess = true;
        try {
            jedis = this.jedisPool.getResource();
            return jedis.lpop(key);
        } catch (Exception e) {
            oprSuccess = false;
            if (jedis != null)
                this.jedisPool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        } finally {
            if (oprSuccess)
                this.jedisPool.returnResource(jedis);

        }
    }

    public Object pipeline(BiFunction<? super Pipeline, ? super Object[], ? super Object> call, Object... args) {
        Jedis jedis = null;
        boolean oprSuccess = true;
        try {
            jedis = this.jedisPool.getResource();
            Pipeline p = jedis.pipelined();

            Object result = (Long) call.apply(p, args);

            p.sync();

            return result;
        } catch (Exception e) {
            oprSuccess = false;
            if (jedis != null)
                this.jedisPool.returnBrokenResource(jedis);
            throw new RuntimeException(e);
        } finally {
            if (oprSuccess)
                this.jedisPool.returnResource(jedis);

        }
    }

    public long expire(String key, int seconds) {
        if (key == null || key.equals("")) {
            return 0;
        }

        Jedis jedis = null;
        try {
            jedis = this.jedisPool.getResource();
            return jedis.expire(key, seconds);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }
}
