package sp.rafael.redis.sample.test;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by rafael on 4/15/16.
 */
@RunWith(JUnitParamsRunner.class)
public class JedisTest {


    static final String JEDIS_HOST="127.0.0.1";
    static final int JEDIS_PORT=6379;
    static final String FEMALES_SET="females";
    Jedis jedis;

    @Before
    public void before(){
        jedis = new Jedis(JEDIS_HOST,JEDIS_PORT);
    }

    @After
    public void after(){
        jedis.disconnect();
        jedis=null;
    }


    @Test
    @Parameters({"mar","kay","ren","tai","nai","joi","cam" })
    public void buscarPeloNome(String searchStr){



        long now = System.currentTimeMillis();
        byte[] prefixByte = ("[" + searchStr).getBytes();
        byte[] prefixByteExtended = Arrays.copyOf(prefixByte, prefixByte.length + 1);
        prefixByteExtended[prefixByte.length] = (byte) 0xFF;
        String min = ("[" + searchStr);
        String max = new String(prefixByteExtended);

        Set<String> found = jedis.zrangeByLex(FEMALES_SET,min, max);
        Iterator<String> it = found.iterator();
        System.out.println(String.format("Pessoas encontradas com %s e %s \n",min,max));

        while(it.hasNext()){
            String nome = it.next();
            System.out.print(nome + ",");
        }

        long dif = System.currentTimeMillis() - now;

        System.out.println(String.format(" run in %s ms\n",dif));

    }


}
