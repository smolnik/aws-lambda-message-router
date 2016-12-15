package cloud.developing.sqs.router;

import java.time.LocalDateTime;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.lambda.runtime.events.SNSEvent.MessageAttribute;
import com.amazonaws.services.lambda.runtime.events.SNSEvent.SNS;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.SendMessageResult;

/**
 * @author asmolnik
 *
 */
public class SqsRouter {

	private final AmazonSQS sqs = new AmazonSQSClient();

	public void route(SNSEvent event, Context context) {
		LambdaLogger log = context.getLogger();
		event.getRecords().forEach(snsRecord -> {
			SNS snsMessage = snsRecord.getSNS();
			String payload = snsMessage.getMessage();
			MessageAttribute returnTo = snsMessage.getMessageAttributes().get("returnTo");
			if (null == returnTo || null == returnTo.getValue()) {
				log.log("returnTo attribute is null so nowhere to send the response back");
				return;
			}
			String returnMessage = payload + "\n" + LocalDateTime.now();
			SendMessageResult result = sqs.sendMessage(returnTo.getValue(), returnMessage);
			log.log("Message has been sent to: " + returnTo + " with content: " + returnMessage + " and result: " + result);
		});

	}

}
