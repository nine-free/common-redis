package test.com.lemall.crd.common.redis;

import com.soft1010.common.redis.RedisService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.UUID;

/**
 * @Author zhangjifu
 * @Create time: 2016/11/2 17:18
 * @Description:
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:conf/test-config.xml", "classpath:spring/applicationContext-redis.xml"})
public class TestRedis {

    @Autowired
    private RedisService redisService;

    @Test
    public void test() {
        String uuid = UUID.randomUUID().toString();
        String key = "test" + uuid;
        redisService.put(key, uuid);
        System.out.println("put key=" + key + " value=" + uuid + " into redis");
        String value = redisService.get(key);
        System.out.println("get key=" + key + " value=" + value);

    }

}
