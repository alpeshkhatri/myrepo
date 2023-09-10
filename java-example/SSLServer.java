import javax.net.ssl.*;
import java.io.*;
import java.security.*;
import java.security.cert.CertificateException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SSLServer {
    private static final int PORT = 12345;
    private static final String KEYSTORE_PATH = "serverKeystore.jks";
    private static final String KEYSTORE_PASSWORD = "password";

    public static void main(String[] args) {
        try {
            // Load the keystore
            KeyStore keyStore = KeyStore.getInstance("JKS");
            FileInputStream keystoreFile = new FileInputStream(KEYSTORE_PATH);
            keyStore.load(keystoreFile, KEYSTORE_PASSWORD.toCharArray());

            // Initialize the SSL context
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(keyStore, KEYSTORE_PASSWORD.toCharArray());

            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(keyStore);

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);

            // Create an SSL server socket
            SSLServerSocketFactory socketFactory = sslContext.getServerSocketFactory();
            SSLServerSocket serverSocket = (SSLServerSocket) socketFactory.createServerSocket(PORT);

            System.out.println("SSL Server started on port " + PORT);

            // Create a thread pool to handle multiple clients
            ExecutorService executorService = Executors.newFixedThreadPool(10);

            while (true) {
                // Wait for a client to connect
                SSLSocket clientSocket = (SSLSocket) serverSocket.accept();
                System.out.println("Client connected from " + clientSocket.getInetAddress());

                // Create a new thread to handle the client
                executorService.execute(new ClientHandler(clientSocket));
            }
        } catch (IOException | NoSuchAlgorithmException | KeyStoreException | CertificateException | UnrecoverableKeyException | KeyManagementException e) {
            e.printStackTrace();
        }
    }

    private static class ClientHandler implements Runnable {
        private final SSLSocket clientSocket;

        public ClientHandler(SSLSocket clientSocket) {
            this.clientSocket = clientSocket;
        }

        @Override
        public void run() {
            try {
                // Set up input and output streams
                BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true);

                // Read and write data to the client
                String message;
                while ((message = reader.readLine()) != null) {
                    System.out.println("Received from client: " + message);
                    writer.println("Server received: " + message);
                }

                // Close the client socket
                clientSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

