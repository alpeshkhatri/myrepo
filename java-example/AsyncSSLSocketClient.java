import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class AsyncSSLSocketClient {

    public static void main(String[] args) {
        int numThreads = 5; // Number of concurrent connections
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

        try {
            for (int i = 0; i < numThreads; i++) {
                Callable<String> clientTask = new ClientTask(i);
                Future<String> future = executorService.submit(clientTask);

                // Handle the result asynchronously (optional)
                future.thenAccept(result -> {
                    System.out.println("Received result from thread " + i + ": " + result);
                });
            }
        } finally {
            executorService.shutdown();
        }
    }

    static class ClientTask implements Callable<String> {
        private final int clientId;

        public ClientTask(int clientId) {
            this.clientId = clientId;
        }

        @Override
        public String call() {
            try {
                // Replace with the hostname and port of the SSL server
                String serverHostname = "example.com";
                int serverPort = 443;

                // Create an SSL socket
                SSLSocketFactory sslSocketFactory = (SSLSocketFactory) SSLSocketFactory.getDefault();
                SSLSocket sslSocket = (SSLSocket) sslSocketFactory.createSocket(serverHostname, serverPort);

                // Perform SSL handshake
                sslSocket.startHandshake();

                PrintWriter out = new PrintWriter(sslSocket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(sslSocket.getInputStream()));

                // Simulate client interaction
                out.println("Hello from Client " + clientId);
                String response = in.readLine();
                System.out.println("Client " + clientId + " received: " + response);

                // Close the SSL socket
                sslSocket.close();

                return response;
            } catch (IOException e) {
                e.printStackTrace();
                return "Error in client " + clientId;
            }
        }
    }
}

