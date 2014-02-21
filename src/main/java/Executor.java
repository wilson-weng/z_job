/**
 * Created by siyuan on 2/20/14.
 */
import java.io.*;
import java.util.List;
import java.util.Random;

import org.apache.zookeeper.*;

public class Executor implements Watcher, Runnable, NodeMonitor.NodeMonitorListener
{
    String myID = String.valueOf(new Random().nextInt(10000));
    NodeMonitor dm;
    ZooKeeper zk;

    public Executor(String hostPort) throws KeeperException, IOException, InterruptedException {
        zk = new ZooKeeper(hostPort, 3000, this);
        dm = new NodeMonitor(zk, myID, null, this);
        initialize();
        zk.exists("/master", true);
        zk.getChildren("/worker/"+myID, true);
    } 
    public void initialize(){
        System.out.println("my id is "+myID);
        try {
            zk.create("/worker", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        try {
            zk.create("/assignment", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        try {
            zk.create("/hr", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        try {
            zk.create("/hr/"+String.valueOf(myID), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (KeeperException e) {

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        try {
            zk.create("/worker/"+String.valueOf(myID), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    /**
     * @param args
     */
    public static void main(String[] args) {
//        if (args.length < 4) {
//            System.err
//                    .println("USAGE: Executor hostPort znode filename program [args ...]");
//            System.exit(2);
//        }
        String hostPort = "localhost:2184";
        try {
            new Executor(hostPort).run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /***************************************************************************
     * We do process any events ourselves, we just need to forward them on.
     *
     * @see org.apache.zookeeper.Watcher(org.apache.zookeeper.proto.WatcherEvent)
     */

    public void process(WatchedEvent event) {
        dm.process(event);
    }

    public void run() {
        try {
            synchronized (this) {
                while (!dm.dead) {
                    wait();
                }
            }
        } catch (InterruptedException e) {
        }
    }

    public void closing(int rc) {
        synchronized (this) {
            notifyAll();
        }
    }

    public void exists() {
        System.out.println("Master is dead! action!");
        try {
            zk.create("/master", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            System.out.println("I'm the master now!");
            masterWork();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            System.out.println("Oh I'm late! ");
        }
    }

    @Override
    public void distributeAssignment(String name, byte[] data) throws KeeperException, InterruptedException, UnsupportedEncodingException {
        System.out.println("start distribute assignment");
        List<String> workers = zk.getChildren("/hr", false);
        int minTask = Integer.MAX_VALUE;
        String target = "";
        for(String worker : workers){
            int assignmentNum = zk.getChildren("/worker/"+worker, false).size();
            if( assignmentNum < minTask){
                minTask = assignmentNum;
                target = worker;
            }
        }
        System.out.println("assignment name: "+name+"  assignment content: "+new String(data, "UTF-8")+"  worker: "+target);
        zk.create("/worker/"+target+"/"+name, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Override
    public void doWork() throws KeeperException, InterruptedException {
        List<String> tasks = zk.getChildren("/worker/"+myID, false);
        for(String task : tasks){
            Thread.sleep(10000);
            System.out.println("finish this job!");
            zk.delete("/assignment/"+task, -1);
            zk.delete("/worker/"+myID+"/"+task, -1);
        }
    }

    public void masterWork() throws KeeperException, InterruptedException {
        zk.delete("/hr/"+myID, -1);
        zk.getChildren("/assignment", true);
    }
}