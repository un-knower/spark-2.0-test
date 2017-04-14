import org.scalatest._

/**
  * Created by yxl on 17/1/18.
  */
abstract class UnitSpec extends FlatSpec with Matchers with
    OptionValues with Inside with Inspectors
