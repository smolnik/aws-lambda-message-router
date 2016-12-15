package cloud.developing.router;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.MessageAttributeValue;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

/**
 * @author asmolnik
 *
 */
public class Main {

	public static void main(String[] args) {
		AmazonSQS sqs = new AmazonSQSClient();
		String returnTo = sqs.getQueueUrl("adams-come-back-home").getQueueUrl();
		
		AmazonSNS sns = new AmazonSNSClient();
		sns.publish(new PublishRequest("arn:aws:sns:us-east-1:xyz", "Hello from Zory!").addMessageAttributesEntry("returnTo",
				new MessageAttributeValue().withDataType("String").withStringValue(returnTo)));

		ScheduledExecutorService ses = Executors.newScheduledThreadPool(1);
		AtomicInteger countEmptyOccurences = new AtomicInteger(0);
		ses.scheduleAtFixedRate(() -> {
			try {
				ReceiveMessageResult result = sqs.receiveMessage(returnTo);
				List<Message> messages = result.getMessages();
				if (messages.isEmpty()) {
					System.out.println("Queue is empty");
					if (countEmptyOccurences.incrementAndGet() > 3) {
						ses.shutdownNow();
					}
					return;
				} else {
					countEmptyOccurences.set(0);
				}

				messages.forEach(message -> {
					System.out.println("received: " + message.getBody());
					sqs.deleteMessage(returnTo, message.getReceiptHandle());
				});
			} catch (Exception e) {
				e.printStackTrace();
				ses.shutdownNow();
			}

		}, 3, 3, TimeUnit.SECONDS);
	}

}
