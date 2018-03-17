package base

import org.slf4j.LoggerFactory

/**
  * Created by yxl on 2018/3/16.
  */
trait Log {
    @transient val logger = LoggerFactory.getLogger(getClass);
}