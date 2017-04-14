package callback;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by yxl on 16/12/8.
 */
public class Send {

	public void send(String msg, CallBack callBack) {
		System.out.println("Send msg:" + msg);

		if (callBack != null) {
			callBack.onComplete("CallBack:" + msg);
		}
	}
}
