package de.tuberlin.cit.test.twittersentiment.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.tuberlin.cit.test.twittersentiment.record.TweetRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;

public class SocketWorker implements Runnable {
	private static final Logger LOG = LoggerFactory.getLogger(SocketWorker.class);
	private ObjectMapper mapper = new ObjectMapper();
	private BlockingQueue<TweetRecord> queue;
	private int port;

	SocketWorker(BlockingQueue<TweetRecord> queue, int port) {
		this.queue = queue;
		this.port = port;
	}

	@Override
	public void run() {
		ServerSocket serverSocket = null;
		Socket socket = null;
		BufferedReader in = null;

		try {
			serverSocket = new ServerSocket(port);
			socket = serverSocket.accept();
			in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

			String line = null;
			while ((line = in.readLine()) != null) {
				JsonNode tweet = mapper.readValue(line, JsonNode.class);
				queue.put(new TweetRecord(tweet));
			}

		} catch (IOException e) {
			LOG.error("I/O error in socket worker.", e);

		} catch (InterruptedException e) {
			LOG.info("Socket worker interrupted.");

		} finally {
			try {
				in.close();
				socket.close();
				serverSocket.close();
			} catch (IOException e) {
				LOG.error("Failed to close socket worker server port.", e);
				e.printStackTrace();
			}
		}
	}
}
