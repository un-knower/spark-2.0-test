package util

import scala.collection.JavaConverters._
import java.net.{Inet4Address, InetAddress, NetworkInterface}

/**
  * Created by yxl on 2018/3/16.
  */
object NetWorkUtil {

     def getLocalAddress(): String = {
        val address = InetAddress.getLocalHost
        if (address.isLoopbackAddress) {
            val activeNetworkIFs = NetworkInterface.getNetworkInterfaces.asScala.toSeq
            val reOrderedNetworkIFs = activeNetworkIFs.reverse
            for (ni <- reOrderedNetworkIFs) {
                val addresses = ni.getInetAddresses.asScala
                .filterNot(addr => addr.isLinkLocalAddress || addr.isLoopbackAddress).toSeq
                if (addresses.nonEmpty) {
                    val addr = addresses.find(_.isInstanceOf[Inet4Address]).getOrElse(addresses.head)
                    val strippedAddress = InetAddress.getByAddress(addr.getAddress)
                    return strippedAddress.getHostAddress
                }
            }
        }
        address.getHostAddress
    }

}
