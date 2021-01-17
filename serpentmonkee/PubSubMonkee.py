# _METADATA_:Version: 20
# _METADATA_:Timestamp: 2021-01-17 21:26:27.466955+00:00
# _METADATA_:MD5: 67765321a8fa1232a3e200abde534474
# _METADATA_:Publish:                      None

# _METADATA_:

from firebase_admin import credentials, firestore
from google.cloud import pubsub_v1
import os
import argparse


class PubSubMonkee:
    def __init__(self, publisher, project_id, topic_id):
        self.publisher = publisher
        self.project_id = project_id
        self.topic_id = topic_id

    def list_topics(self):
        """Lists all Pub/Sub topics in the given project."""

        project_path = f"projects/{self.project_id}"

        for topic in self.publisher.list_topics(request={"project": project_path}):
            print(topic)

    def create_topic(self):
        """Create a new Pub/Sub topic."""

        self.publisher = pubsub_v1.PublisherClient()
        topic_path = self.publisher.topic_path(self.project_id, self.topic_id)

        topic = self.publisher.create_topic(request={"name": topic_path})

        print("Created topic: {}".format(topic.name))

    def delete_topic(self):
        """Deletes an existing Pub/Sub topic."""

        self.publisher = pubsub_v1.PublisherClient()
        topic_path = self.publisher.topic_path(self.project_id, self.topic_id)

        self.publisher.delete_topic(request={"topic": topic_path})

        print("Topic deleted: {}".format(topic_path))

    def publish_message(self, messageData):
        """Publish messages to a Pub/Sub topic."""

        self.publisher = pubsub_v1.PublisherClient()
        # The `topic_path` method creates a fully qualified identifier
        # in the form `projects/{self.project_id}/topics/{self.topic_id}`
        topic_path = self.publisher.topic_path(self.project_id, self.topic_id)

        data = messageData.encode("utf-8")
        # When you publish a message, the client returns a future.
        future = self.publisher.publish(topic_path, data)
        print(future.result())

        print(f"Published message to {topic_path}.")

    def publish_test_messages(self):
        """Publishes multiple messages to a Pub/Sub topic."""

        self.publisher = pubsub_v1.PublisherClient()
        # The `topic_path` method creates a fully qualified identifier
        # in the form `projects/{self.project_id}/topics/{self.topic_id}`
        topic_path = self.publisher.topic_path(self.project_id, self.topic_id)

        for n in range(1, 100):
            data = "Message number {}".format(n)
            # Data must be a bytestring
            data = data.encode("utf-8")
            # When you publish a message, the client returns a future.
            future = self.publisher.publish(topic_path, data)
            print(future.result())

        print(f"Published messages to {topic_path}.")

    def publish_messages_with_custom_attributes(self):
        """Publishes multiple messages with custom attributes
        to a Pub/Sub topic."""

        self.publisher = pubsub_v1.PublisherClient()
        topic_path = self.publisher.topic_path(self.project_id, self.topic_id)

        for n in range(1, 10):
            data = "Message number {}".format(n)
            # Data must be a bytestring
            data = data.encode("utf-8")
            # Add two attributes, origin and username, to the message
            future = self.publisher.publish(
                topic_path, data, origin="python-sample", username="gcp"
            )
            print(future.result())

        print(f"Published messages with custom attributes to {topic_path}.")

    def publish_messages_with_error_handler(self):
        """Publishes multiple messages to a Pub/Sub topic with an error handler."""
        import time

        self.publisher = pubsub_v1.PublisherClient()
        topic_path = self.publisher.topic_path(self.project_id, self.topic_id)

        futures = dict()

        def get_callback(f, data):
            def callback(f):
                try:
                    print(f.result())
                    futures.pop(data)
                except:  # noqa
                    print("Please handle {} for {}.".format(f.exception(), data))

            return callback

        for i in range(10):
            data = str(i)
            futures.update({data: None})
            # When you publish a message, the client returns a future.
            future = self.publisher.publish(topic_path, data.encode("utf-8"))
            futures[data] = future
            # Publish failures shall be handled in the callback function.
            future.add_done_callback(get_callback(future, data))

        # Wait for all the publish futures to resolve before exiting.
        while futures:
            time.sleep(5)

        print(f"Published messages with error handler to {topic_path}.")

    def publish_messages_with_batch_settings(self):
        """Publishes multiple messages to a Pub/Sub topic with batch settings."""

        # Configure the batch to publish as soon as there is ten messages,
        # one kilobyte of data, or one second has passed.
        batch_settings = pubsub_v1.types.BatchSettings(
            max_messages=10,  # default 100
            max_bytes=1024,  # default 1 MB
            max_latency=1,  # default 10 ms
        )
        self.publisher = pubsub_v1.PublisherClient(batch_settings)
        topic_path = self.publisher.topic_path(self.project_id, self.topic_id)

        # Resolve the publish future in a separate thread.
        def callback(future):
            message_id = future.result()
            print(message_id)

        for n in range(1, 10):
            data = "Message number {}".format(n)
            # Data must be a bytestring
            data = data.encode("utf-8")
            future = self.publisher.publish(topic_path, data)
            # Non-blocking. Allow the self.publisher client to batch multiple messages.
            future.add_done_callback(callback)

        print(f"Published messages with batch settings to {topic_path}.")

    def publish_messages_with_retry_settings(self):
        """Publishes messages with custom retry settings."""
        from google import api_core

        # Configure the retry settings. Defaults shown in comments are values applied
        # by the library by default, instead of default values in the Retry object.
        custom_retry = api_core.retry.Retry(
            initial=0.250,  # seconds (default: 0.1)
            maximum=90.0,  # seconds (default: 60.0)
            multiplier=1.45,  # default: 1.3
            deadline=300.0,  # seconds (default: 60.0)
            predicate=api_core.retry.if_exception_type(
                api_core.exceptions.Aborted,
                api_core.exceptions.DeadlineExceeded,
                api_core.exceptions.InternalServerError,
                api_core.exceptions.ResourceExhausted,
                api_core.exceptions.ServiceUnavailable,
                api_core.exceptions.Unknown,
                api_core.exceptions.Cancelled,
            ),
        )

        self.publisher = pubsub_v1.PublisherClient()
        topic_path = self.publisher.topic_path(self.project_id, self.topic_id)

        for n in range(1, 100):
            data = "Message number {}".format(n)
            # Data must be a bytestring
            data = data.encode("utf-8")
            future = self.publisher.publish(
                topic=topic_path, data=data, retry=custom_retry)
            print(future.result())

        print(f"Published messages with retry settings to {topic_path}.")

    def publish_with_ordering_keys(self):
        """Publishes messages with ordering keys."""

        publisher_options = pubsub_v1.types.PublisherOptions(
            enable_message_ordering=True)
        # Sending messages to the same region ensures they are received in order
        # even when multiple publishers are used.
        client_options = {"api_endpoint": "us-east1-pubsub.googleapis.com:443"}
        self.publisher = pubsub_v1.PublisherClient(
            publisher_options=publisher_options, client_options=client_options
        )
        # The `topic_path` method creates a fully qualified identifier
        # in the form `projects/{self.project_id}/topics/{self.topic_id}`
        topic_path = self.publisher.topic_path(self.project_id, self.topic_id)

        for message in [
            ("message1", "key1"),
            ("message2", "key2"),
            ("message3", "key1"),
            ("message4", "key2"),
        ]:
            # Data must be a bytestring
            data = message[0].encode("utf-8")
            ordering_key = message[1]
            # When you publish a message, the client returns a future.
            future = self.publisher.publish(
                topic_path, data=data, ordering_key=ordering_key)
            print(future.result())

        print(f"Published messages with ordering keys to {topic_path}.")

    def resume_publish_with_ordering_keys(self):
        """Resume publishing messages with ordering keys when unrecoverable errors occur."""

        publisher_options = pubsub_v1.types.PublisherOptions(
            enable_message_ordering=True)
        # Sending messages to the same region ensures they are received in order
        # even when multiple publishers are used.
        client_options = {"api_endpoint": "us-east1-pubsub.googleapis.com:443"}
        self.publisher = pubsub_v1.PublisherClient(
            publisher_options=publisher_options, client_options=client_options
        )
        # The `topic_path` method creates a fully qualified identifier
        # in the form `projects/{self.project_id}/topics/{self.topic_id}`
        topic_path = self.publisher.topic_path(self.project_id, self.topic_id)

        for message in [
            ("message1", "key1"),
            ("message2", "key2"),
            ("message3", "key1"),
            ("message4", "key2"),
        ]:
            # Data must be a bytestring
            data = message[0].encode("utf-8")
            ordering_key = message[1]
            # When you publish a message, the client returns a future.
            future = self.publisher.publish(
                topic_path, data=data, ordering_key=ordering_key)
            try:
                print(future.result())
            except RuntimeError:
                # Resume publish on an ordering key that has had unrecoverable errors.
                self.publisher.resume_publish(topic_path, ordering_key)

        print(
            f"Resumed publishing messages with ordering keys to {topic_path}.")

    def doer(self, command):
        if command == "list":
            self.list_topics()
        elif command == "create":
            self.create_topic()
        elif command == "delete":
            self.delete_topic()
        elif command == "publish":
            self.publish_message('testing')
        elif command == "publish-with-custom-attributes":
            self.publish_messages_with_custom_attributes()
        elif command == "publish-with-error-handler":
            self.publish_messages_with_error_handler()
        elif command == "publish-with-batch-settings":
            self.publish_messages_with_batch_settings()
        elif command == "publish-with-retry-settings":
            self.publish_messages_with_retry_settings()
        elif command == "publish-with-ordering-keys":
            self.publish_with_ordering_keys()
        elif command == "resume-publish-with-ordering-keys":
            self.resume_publish_with_ordering_keys()
        # elif command == "detach-subscription":
        #    detach_subscription(self.project_id, args.subscription_id)


if __name__ == "__main__":

    command = 'publish'
    topic_id = 'some_topic'
