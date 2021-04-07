import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class LeaderElection implements Watcher {
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private static final String ELECTION_NAMESPACE = "/election";
    private static final String TARGET_ZNODE = "/target_znode";
    private String currentZnodeName;
    private ZooKeeper zooKeeper;

    private static void main(String[] args)  throws IOException, InterruptedException, KeeperException {
        LeaderElection leaderElection = new LeaderElection();
        leaderElection.connectToZooKeeper();
        leaderElection.volenteerForLeadership();
        leaderElection.reelectLeader();
        leaderElection.run();
        leaderElection.close();
        System.out.println("Exiting from application");
    }

    public void volenteerForLeadership() throws KeeperException, InterruptedException{
        String znodePrefix = ELECTION_NAMESPACE + "/c_";
        String znodeFullPath =  zooKeeper.create(znodePrefix , new byte[]{} , ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL );

        System.out.println("znode name" + znodeFullPath);
        this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
    }

    public void connectToZooKeeper() throws IOException{
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS,SESSION_TIMEOUT, this);
    }

    public void reelectLeader() throws KeeperException , InterruptedException{
        Stat prodecessorStat = null;
        String predecessorZnodeName = "";
        while ( prodecessorStat == null ) {
            List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
            Collections.sort(children);
            String smallestChild = children.get(0);


            if (smallestChild.equals(currentZnodeName)) {
                System.out.println("Leader Found");
                return;
            } else {
                System.out.println(" I am not the leader ");
                int predeccossorIndex = Collections.binarySearch(children, currentZnodeName);
                predecessorZnodeName = children.get(predeccossorIndex);
                prodecessorStat = zooKeeper.exists(ELECTION_NAMESPACE + "/" + predecessorZnodeName, this);

            }
        }
        System.out.println("Watching znode " + predecessorZnodeName);
    }

    public void run() throws InterruptedException{
        synchronized (zooKeeper){
            zooKeeper.wait();
        }
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    public void watchTargetZnode() throws KeeperException , InterruptedException {
        Stat stat = zooKeeper.exists(TARGET_ZNODE, this);
        if(stat == null){
            return;
        }
        byte [] data = zooKeeper.getData(TARGET_ZNODE, this, stat);
        List<String> children = zooKeeper.getChildren(TARGET_ZNODE, this);

        System.out.println("Data: " + new String(data) + " children :" + children);
    }

    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()){
            case  None:
                if(event.getState() == Event.KeeperState.SyncConnected){
                    System.out.println("Successfully connected");
                }else{
                    synchronized (zooKeeper){
                        System.out.println("System disconnected");
                        zooKeeper.notifyAll();
                    }
                }
                break;
            case NodeDeleted:
                System.out.println(TARGET_ZNODE + " was deleted ");
                try{
                    reelectLeader();
                }catch (KeeperException e){
                    e.printStackTrace();
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
                break;
        }
    }
}
