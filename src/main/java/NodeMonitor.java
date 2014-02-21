/**
 * Created by siyuan on 2/20/14.
 */
/**
 * Created by siyuan on 2/20/14.
 */
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;

import org.apache.zookeeper.*;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

import static org.apache.zookeeper.Watcher.Event.EventType.NodeDeleted;
import static org.apache.zookeeper.Watcher.Event.EventType.None;

public class NodeMonitor implements Watcher, StatCallback {

    ZooKeeper zk;

    String myId;

    Watcher chainedWatcher;

    boolean dead;

    NodeMonitorListener listener;

    byte prevData[];

    public NodeMonitor(ZooKeeper zk, String myId, Watcher chainedWatcher,
                           NodeMonitorListener listener) {
        this.zk = zk;
        this.myId = myId;
        this.chainedWatcher = chainedWatcher;
        this.listener = listener;
        // Get things started by checking if the node exists. We are going
        // to be completely event driven
        zk.exists("/master", true, this, null);
    }

    /**
     * Other classes use the DataMonitor by implementing this method
     */
    public interface NodeMonitorListener {
        /**
         * The existence status of the node has changed.
         */
        void exists();
        void distributeAssignment(String name, byte[] data) throws KeeperException, InterruptedException, UnsupportedEncodingException;
        void doWork() throws KeeperException, InterruptedException;
        /**
         * The ZooKeeper session is no longer valid.
         *
         * @param rc
         *                the ZooKeeper reason code
         */
        void closing(int rc);
    }

    public void process(WatchedEvent event) {
        String path = event.getPath();
        if (event.getType() == Event.EventType.None) {
            // We are are being told that the state of the
            // connection has changed
            switch (event.getState()) {
                case SyncConnected:
                    // In this particular example we don't need to do anything
                    // here - watches are automatically re-registered with
                    // server and any watches triggered while the client was
                    // disconnected will be delivered (in order of course)
                    break;
                case Expired:
                    // It's all over
                    dead = true;
                    listener.closing(KeeperException.Code.SessionExpired);
                    break;
            }
        } else {
            switch (event.getType()){
                case NodeChildrenChanged:
                    try {
                        String eventPath = event.getPath();
                        if(eventPath.contains("/assignment")){
                            System.out.println("new assignment comes!");
                            List<String> tasks = zk.getChildren("/assignment", true);
                            for(String task : tasks){
                                byte[] data = zk.getData(eventPath+"/"+task, false,null);
                                listener.distributeAssignment(task, data);
                            }

                        }else if (eventPath.contains("/worker")){
                            System.out.println("get a work to do!");
                            zk.getChildren("/worker/"+myId, true);
                            listener.doWork();
                        }

                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                    break;
                case NodeDeleted:
                    if (path != null && path.equals("/master")) {
                        // Something has changed on the node, let's find out
                        zk.exists("/master", true, this, null);
                    }

            }
        }
        if (chainedWatcher != null) {
            chainedWatcher.process(event);
        }
    }

    public void processResult(int rc, String path, Object ctx, Stat stat) {
        boolean exists;
        switch (rc) {
            case Code.Ok:
                exists = true;
                break;
            case Code.NoNode:
                exists = false;
                break;
            case Code.SessionExpired:
            case Code.NoAuth:
                dead = true;
                listener.closing(rc);
                return;
            default:
                // Retry errors
                zk.exists("/master", true, this, null);
                return;
        }

        if (!exists) {
            listener.exists();
        }
    }
}

