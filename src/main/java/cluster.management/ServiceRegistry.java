package cluster.management;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ServiceRegistry implements Watcher {
    private static final String REGISTRY_ZNODE = "/service_registry";
    private final ZooKeeper zookeeper;
    private String currentZnode = null;
    private  List<String> allServicesAddressess = null;

    public ServiceRegistry(ZooKeeper zooKeeper ){
        this.zookeeper = zooKeeper;
        createServiceRegistryZnode();
    }

    public void registerToCluster(String metadata ) throws  KeeperException, InterruptedException {

            this.currentZnode = zookeeper.create(REGISTRY_ZNODE + "/n" ,metadata.getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println(" Registered to Service Registry ");
    }

    public void registerForUpdates(){
        try {
            updateAddresses();
        } catch (KeeperException e){
            e.printStackTrace();
        }catch (InterruptedException e){
            e.printStackTrace();
        }
    }

    public synchronized List<String> getAllServicesAddressess() throws KeeperException, InterruptedException{
        if (allServicesAddressess == null ){
            updateAddresses();
        }
        return allServicesAddressess;
    }

    public void unregisterFromCluster() throws KeeperException, InterruptedException{
        if (currentZnode != null && zookeeper.exists(currentZnode, false) != null) {
            zookeeper.delete(currentZnode, -1 );
        }
    }

    public void createServiceRegistryZnode(){
        try {
            if (zookeeper.exists(REGISTRY_ZNODE, false) == null) {
                zookeeper.create(REGISTRY_ZNODE, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        }catch (KeeperException e ){
                e.printStackTrace();
        }
        catch (InterruptedException e){
            e.printStackTrace();

        }
    }

    private synchronized void updateAddresses() throws  KeeperException, InterruptedException{
        List<String> workerZnodes = zookeeper.getChildren(REGISTRY_ZNODE,this);
        List<String> addresses = new ArrayList<String>(workerZnodes.size());

        for (String workderZnode : addresses ){
            String workerZnodeFullPath = REGISTRY_ZNODE + "/" + workderZnode;
            Stat stat = zookeeper.exists(workerZnodeFullPath , false);
            if ( stat == null ){
                continue;
            }

            byte [] addressBytes = zookeeper.getData(workerZnodeFullPath, false, stat );
            String address = new String(addressBytes);
            addresses.add(address);
        }

        this.allServicesAddressess = Collections.unmodifiableList(addresses);
        System.out.println("The Cluster addresses are : " + this.allServicesAddressess );
    }

    @Override
    public void process(WatchedEvent event ){
        try {
            updateAddresses();
        } catch (KeeperException e){
            e.printStackTrace();
        } catch (InterruptedException e){
            e.printStackTrace();
        }
    }
}

