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
        // String for name of pred node
        String predecessorZnodeName = "";
        // while loop to help prevent race conditions
        while ( prodecessorStat == null ) {
            List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
            Collections.sort(children);
            String smallestChild = children.get(0);

            if (smallestChild.equals(currentZnodeName)) {
                System.out.println("Leader Found");
                return;
            } else {
                // Current block find the predesccossor Node in case that Node fails in order to increase fault tolerence
                System.out.println(" I am not the leader ");
                // uses a binary Search to find the current Node index in the list of children -1 as you are searching for the predeccossor
                int predeccossorIndex = Collections.binarySearch(children, currentZnodeName) -1;
                // Getting the name of the last Znode
                predecessorZnodeName = children.get(predeccossorIndex);
                // checking if that Znode exists or still exisits
                prodecessorStat = zooKeeper.exists(ELECTION_NAMESPACE + "/" + predecessorZnodeName, this);
            }
        }
        // Print out the current Znode in which you are watching
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
        // stat -> Gives all of the current Meta Data about the Znode

        Stat stat = zooKeeper.exists(TARGET_ZNODE, this);

        // If the current Znode does not currently exist then return from the method
        if(stat == null){
            return;
        }
        // if it does exist then call the current data into a list of bytes and implement a watcher method

        byte [] data = zooKeeper.getData(TARGET_ZNODE, this, stat);
        // calling the getChildren to order to notify any changes to the current Node children

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

                // Creating a cass for a NodeDeletion event to notify connected nodes of a deletion
                // This force the current node to relellect leader if a node in the network fails
                // This help to prevent the herd affect
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
