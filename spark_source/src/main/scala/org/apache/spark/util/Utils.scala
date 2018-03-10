/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util

import java.io.{File, IOException}
import java.net.{BindException, URI}
import java.nio.{MappedByteBuffer, ByteBuffer}
import java.util.UUID

import org.apache.spark.internal.Logging
import org.apache.spark._
import org.apache.spark.util.JavaUtils
import org.eclipse.jetty.util.MultiException
import sun.nio.ch.DirectBuffer
import scala.collection.JavaConverters._
import scala.util.Random
import scala.util.control.NonFatal


/**
  * Various utility methods used by Spark.
  */
private[spark] object Utils extends Logging {
    val random = new Random()

    /**
      * Get the Context ClassLoader on this thread or, if not present, the ClassLoader that
      * loaded Spark.
      *
      * This should be used whenever passing a ClassLoader to Class.ForName or finding the currently
      * active loader when setting up ClassLoader delegation chains.
      */
    def getContextOrSparkClassLoader: ClassLoader =
        Option(Thread.currentThread().getContextClassLoader).getOrElse(getSparkClassLoader)


    def classForName(className: String): Class[_] = {
        Class.forName(className, true, getContextOrSparkClassLoader)
    }

    /**
      * Get the ClassLoader which loaded Spark.
      */
    def getSparkClassLoader: ClassLoader = getClass.getClassLoader

    /**
      * Return a pair of host and port extracted from the `sparkUrl`.
      *
      * A spark url (`spark://host:port`) is a special URI that its scheme is `spark` and only contains
      * host and port.
      *
      * @throws SparkException if `sparkUrl` is invalid.
      */
    def extractHostPortFromSparkUrl(sparkUrl: String): (String, Int) = {
        try {
            val uri = new java.net.URI(sparkUrl)
            val host = uri.getHost
            val port = uri.getPort
            if (uri.getScheme != "spark" ||
            host == null ||
            port < 0 ||
            (uri.getPath != null && !uri.getPath.isEmpty) || // uri.getPath returns "" instead of null
            uri.getFragment != null ||
            uri.getQuery != null ||
            uri.getUserInfo != null) {
                throw new SparkException("Invalid master URL: " + sparkUrl)
            }
            (host, port)
        } catch {
            case e: java.net.URISyntaxException =>
                throw new SparkException("Invalid master URL: " + sparkUrl, e)
        }
    }


    /**
      * Return the string to tell how long has passed in milliseconds.
      */
    def getUsedTimeMs(startTimeMs: Long): String = {
        " " + (System.currentTimeMillis - startTimeMs) + " ms"
    }


    /**
      * Convert a time parameter such as (50s, 100ms, or 250us) to seconds for internal use. If
      * no suffix is provided, the passed number is assumed to be in seconds.
      */
    def timeStringAsSeconds(str: String): Long = {
        JavaUtils.timeStringAsSec(str)
    }

    def checkHost(host: String, message: String = "") {
        assert(host.indexOf(':') == -1, message)
    }

    def checkHostPort(hostPort: String, message: String = "") {
        assert(hostPort.indexOf(':') != -1, message)
    }

    /**
      * Convert a time parameter such as (50s, 100ms, or 250us) to microseconds for internal use. If
      * no suffix is provided, the passed number is assumed to be in ms.
      */
    def timeStringAsMs(str: String): Long = {
        JavaUtils.timeStringAsMs(str)
    }


    /**
      * A file name may contain some invalid URI characters, such as " ". This method will convert the
      * file name to a raw path accepted by `java.net.URI(String)`.
      *
      * Note: the file name must not contain "/" or "\"
      */
    def encodeFileNameToURIRawPath(fileName: String): String = {
        require(!fileName.contains("/") && !fileName.contains("\\"))
        // `file` and `localhost` are not used. Just to prevent URI from parsing `fileName` as
        // scheme or host. The prefix "/" is required because URI doesn't accept a relative path.
        // We should remove it after we get the raw path.
        new URI("file", null, "localhost", -1, "/" + fileName, null, null).getRawPath.substring(1)
    }

    /**
      * Attempt to clean up a ByteBuffer if it is memory-mapped. This uses an *unsafe* Sun API that
      * might cause errors if one attempts to read from the unmapped buffer, but it's better than
      * waiting for the GC to find it because that could lead to huge numbers of open files. There's
      * unfortunately no standard API to do this.
      */
    def dispose(buffer: ByteBuffer): Unit = {
        if (buffer != null && buffer.isInstanceOf[MappedByteBuffer]) {
            logTrace(s"Unmapping $buffer")
            if (buffer.asInstanceOf[DirectBuffer].cleaner() != null) {
                buffer.asInstanceOf[DirectBuffer].cleaner().clean()
            }
        }
    }

    /**
      * Execute a block of code that returns a value, re-throwing any non-fatal uncaught
      * exceptions as IOException. This is used when implementing Externalizable and Serializable's
      * read and write methods, since Java's serializer will not report non-IOExceptions properly;
      * see SPARK-4080 for more context.
      */
    def tryOrIOException[T](block: => T): T = {
        try {
            block
        } catch {
            case e: IOException =>
                logError("Exception encountered", e)
                throw e
            case NonFatal(e) =>
                logError("Exception encountered", e)
                throw new IOException(e)
        }
    }

    /**
      * Maximum number of retries when binding to a port before giving up.
      */
    def portMaxRetries(conf: SparkConf): Int = {
        val maxRetries = conf.getOption("spark.port.maxRetries").map(_.toInt)
        if (conf.contains("spark.testing")) {
            // Set a higher number of retries for tests...
            maxRetries.getOrElse(100)
        } else {
            maxRetries.getOrElse(16)
        }
    }

    /**
      * Return whether the exception is caused by an address-port collision when binding.
      */
    def isBindCollision(exception: Throwable): Boolean = {
        exception match {
            case e: BindException =>
                if (e.getMessage != null) {
                    return true
                }
                isBindCollision(e.getCause)
            case e: MultiException =>
                e.getThrowables.asScala.exists(isBindCollision)
            case e: Exception => isBindCollision(e.getCause)
            case _ => false
        }
    }

    /**
      * Attempt to start a service on the given port, or fail after a number of attempts.
      * Each subsequent attempt uses 1 + the port used in the previous attempt (unless the port is 0).
      *
      * @param startPort The initial port to start the service on.
      * @param startService Function to start service on a given port.
      *                     This is expected to throw java.net.BindException on port collision.
      * @param conf A SparkConf used to get the maximum number of retries when binding to a port.
      * @param serviceName Name of the service.
      * @return (service: T, port: Int)
      */
    def startServiceOnPort[T](
                             startPort: Int,
                             startService: Int => (T, Int),
                             conf: SparkConf,
                             serviceName: String = ""): (T, Int) = {

        require(startPort == 0 || (1024 <= startPort && startPort < 65536),
            "startPort should be between 1024 and 65535 (inclusive), or 0 for a random free port.")

        val serviceString = if (serviceName.isEmpty) "" else s" '$serviceName'"
        val maxRetries = portMaxRetries(conf)
        for (offset <- 0 to maxRetries) {
            // Do not increment port if startPort is 0, which is treated as a special port
            val tryPort = if (startPort == 0) {
                startPort
            } else {
                // If the new port wraps around, do not try a privilege port
                ((startPort + offset - 1024) % (65536 - 1024)) + 1024
            }
            try {
                val (service, port) = startService(tryPort)
                logInfo(s"Successfully started service$serviceString on port $port.")
                return (service, port)
            } catch {
                case e: Exception if isBindCollision(e) =>
                    if (offset >= maxRetries) {
                        val exceptionMessage = s"${e.getMessage}: Service$serviceString failed after " +
                        s"$maxRetries retries! Consider explicitly setting the appropriate port for the " +
                        s"service$serviceString (for com.example spark.ui.port for SparkUI) to an available " +
                        "port or increasing spark.port.maxRetries."
                        val exception = new BindException(exceptionMessage)
                        // restore original stack trace
                        exception.setStackTrace(e.getStackTrace)
                        throw exception
                    }
                    logWarning(s"Service$serviceString could not bind on port $tryPort. " +
                    s"Attempting port ${tryPort + 1}.")
            }
        }
        // Should never happen
        throw new SparkException(s"Failed to start service$serviceString on port $startPort")
    }


    /**
      * Create a temporary directory inside the given parent directory. The directory will be
      * automatically deleted when the VM shuts down.
      */
    def createTempDir(
                     root: String = System.getProperty("java.io.tmpdir"),
                     namePrefix: String = "spark"): File = {
        val dir = createDirectory(root, namePrefix)
        dir
    }

    /**
      * Create a directory inside the given parent directory. The directory is guaranteed to be
      * newly created, and is not marked for automatic deletion.
      */
    def createDirectory(root: String, namePrefix: String = "spark"): File = {
        var attempts = 0
        val maxAttempts = 5
        var dir: File = null
        while (dir == null) {
            attempts += 1
            if (attempts > maxAttempts) {
                throw new IOException("Failed to create a temp directory (under " + root + ") after " +
                maxAttempts + " attempts!")
            }
            try {
                dir = new File(root, namePrefix + "-" + UUID.randomUUID.toString)
                if (dir.exists() || !dir.mkdirs()) {
                    dir = null
                }
            } catch { case e: SecurityException => dir = null; }
        }

        dir.getCanonicalFile
    }
}
