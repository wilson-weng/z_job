import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import java.util.Random;

/**
 * Created by siyuan on 2/21/14.
 */
public class testClient {
    @Test
    public void test() throws Exception{
        ZooKeeper zk = new ZooKeeper("localhost:2184", 3000, null);
        for(int i = 0; i<100; i++){
            String random = String.valueOf(new Random().nextInt(10000));
            zk.create("/assignment/"+random, random.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println("add assignment: "+random);
        }
    }
}
