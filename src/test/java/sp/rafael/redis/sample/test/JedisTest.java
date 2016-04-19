package sp.rafael.redis.sample.test;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import redis.clients.jedis.Jedis;

import java.util.*;

/**
 * Created by rafael on 4/15/16.
 */
@RunWith(JUnitParamsRunner.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class JedisTest {


    static final String JEDIS_HOST = "127.0.0.1";
    static final int JEDIS_PORT = 6379;
    static final String NAMES_SET = "females";
    Jedis jedis;

    @Before
    public void before() {
        jedis = new Jedis(JEDIS_HOST, JEDIS_PORT);
    }

    @After
    public void after() {
        jedis.disconnect();
        jedis = null;
    }


    @Test
    public void t01_gerarNomes() {
        final long COUNT = 15000L;
        for (long i = 0; i < COUNT; i++) {
            final String nome = getRandomlyNames(50, 1)[0].toUpperCase();
            jedis.zadd(NAMES_SET, 0, nome+":::"+i);
        }

    }

    @Test
    @Parameters({"MAR", "KAY", "REN", "TAI", "NAI", "JOI", "CAM"})
    public void t02_buscarPeloNome(String searchStr) {


        byte[] prefixByte = ("[" + searchStr).getBytes();
        byte[] prefixByteExtended = Arrays.copyOf(prefixByte, prefixByte.length + 1);
        prefixByteExtended[prefixByte.length] = (byte) 0xFF;
        String min = ("[" + searchStr);
        String max = new String(prefixByteExtended);

        long now = System.currentTimeMillis();
        Set<String> found = jedis.zrangeByLex(NAMES_SET, min, max);
        long dif = System.currentTimeMillis() - now;

        Iterator<String> it = found.iterator();
        long zcard = jedis.zcard(NAMES_SET);
        System.out.println(String.format("Names found com %s e %s, within %s names \n", min, max,zcard));

        while (it.hasNext()) {
            String nome = it.next();
            System.out.print(nome + ",");
        }

        long zfound = jedis.zlexcount(NAMES_SET,min,max);

        System.out.println(String.format("\nFound %s, run in %s ms\n", zfound,dif));

    }

    public String[] getRandomlyNames(final int characterLength, final int generateSize) {
        LinkedHashSet<String> list = new LinkedHashSet<String>();
        for (int i = 0; i < generateSize; ++i) {
            String name = null;
            do {
                name = org.apache.commons.lang.RandomStringUtils.randomAlphabetic(
                        org.apache.commons.lang.math.RandomUtils.nextInt(characterLength - 1) + 1);
            } while (list.contains(name));
            list.add(name);
        }
        return list.toArray(new String[]{});
    }

    public static void main(String args[]) {
        Scanner s = new Scanner(System.in);
        String inputString;
        JedisTest test = new JedisTest();


        do {
            System.out.println("Type the name you want to search and strike <ENTER>: ");
            inputString = s.nextLine();
            test.before();
            test.t01_gerarNomes();
            test.t02_buscarPeloNome(inputString.toUpperCase());
            test.after();
        } while (true);


    }


}
