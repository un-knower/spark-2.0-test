import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import com.util.RedisUtil

/**
  * Created by yxl on 17/4/17.
  */
class RedisSuite  extends FunSuite with Matchers with BeforeAndAfter{


  test("redis sentinel connection"){

    val pool = RedisUtil.getPool()
    val connection = pool.getResource

    connection.hset("hset-1","work","2")
    connection.close()
    RedisUtil.close(pool)

  }



}
