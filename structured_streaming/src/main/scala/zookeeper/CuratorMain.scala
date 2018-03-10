package zookeeper

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry

/**
  * Created by yxl on 17/5/16.
  */
object CuratorMain {

    def main(args: Array[String]) {

        val retryPolicy = new ExponentialBackoffRetry(1000, 3)
        val client = CuratorFrameworkFactory.newClient("127.0.0.1:2181:", retryPolicy);
        client.start()

    }


}
