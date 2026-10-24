package com.babelqueue.redis;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.babelqueue.JsonValues;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.protocol.ProtocolVersion;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The byte-exact path through a <b>real</b> Lettuce connection (the mocks in {@link RecordingRedis} cannot prove
 * that {@code dispatch} + {@code ByteArrayCodec} really keep bytes): a loopback socket speaks just enough RESP2 to
 * answer the queue scripts with a value that is not valid UTF-8 and records the exact bytes it is sent back. No
 * Redis, no network beyond the loopback interface.
 */
class RedisByteExactWireTest {

    /** {"a":"<0xFF>"}: not UTF-8, so a text codec would turn the 0xFF byte into U+FFFD. */
    private static final byte[] BAD = {'{', '"', 'a', '"', ':', '"', (byte) 0xFF, '"', '}'};

    private ServerSocket server;
    private Thread acceptor;
    private final List<List<byte[]>> commands = Collections.synchronizedList(new ArrayList<>());
    private RedisClient client;
    private StatefulRedisConnection<String, String> connection;

    @BeforeEach
    void start() throws Exception {
        server = new ServerSocket(0, 5, InetAddress.getLoopbackAddress());
        acceptor = new Thread(this::serve, "fake-redis");
        acceptor.setDaemon(true);
        acceptor.start();
        client = RedisClient.create(RedisURI.create("redis://127.0.0.1:" + server.getLocalPort()));
        client.setOptions(ClientOptions.builder().protocolVersion(ProtocolVersion.RESP2).build());
        connection = client.connect();
    }

    @AfterEach
    void stop() throws IOException {
        if (connection != null) {
            connection.close();
        }
        if (client != null) {
            client.shutdown();
        }
        server.close();
    }

    @Test
    void legacyReserveAckAndDeadLetterCarryTheExactBytesOverTheWire() {
        RedisQueueOps ops = new RedisQueueOps(connection.sync(), "orders", RedisLayout.LEGACY, null, 90L, -1, () -> 1000.0);
        Reservation r = ops.reserve();
        assertNotNull(r);
        assertTrue(r.hasInvalidUtf8());
        assertArrayEquals(BAD, r.bodyBytes());

        assertTrue(ops.ack(r));
        List<byte[]> ack = lastCommand("EVALSHA", RedisLua.sha1(RedisLua.LEGACY_ACK));
        // EVALSHA sha numkeys key key arg arg: the last argument is the body, byte for byte.
        assertArrayEquals(BAD, ack.get(ack.size() - 1));
        assertArrayEquals(r.exactLegacyMember(), ack.get(ack.size() - 2));

        assertTrue(ops.deadLetter(r));
        List<byte[]> move = lastCommand("EVALSHA", RedisLua.sha1(RedisLua.LEGACY_MOVE));
        assertArrayEquals(BAD, move.get(move.size() - 1));
        assertArrayEquals(BAD, move.get(move.size() - 2));
        for (List<byte[]> command : commands) {
            for (byte[] part : command) {
                assertFalse(contains(part, new byte[] {(byte) 0xEF, (byte) 0xBF, (byte) 0xBD}),
                    "no U+FFFD (EF BF BD) ever goes on the wire");
            }
        }
    }

    @Test
    void zsetReserveAndAckCarryTheExactBytesOverTheWire() {
        RedisQueueOps ops = new RedisQueueOps(connection.sync(), "orders", RedisLayout.ZSET, null, 90L, -1, () -> 1000.0);
        Reservation r = ops.reserve();
        assertNotNull(r);
        assertTrue(r.poison());
        assertArrayEquals(BAD, r.bodyBytes());
        assertArrayEquals(BAD, r.handleBytes());

        assertTrue(ops.ack(r));
        List<byte[]> zrem = lastCommand("ZREM", null);
        assertEquals("queues:orders:reserved", new String(zrem.get(1), StandardCharsets.UTF_8));
        assertArrayEquals(BAD, zrem.get(2));
    }

    @Test
    void aValidUtf8ValueStaysOnTheTextPathAndIsDecodedNormally() {
        // The same server, but the reserve answers valid UTF-8: the reservation carries no byte copy.
        RedisQueueOps ops = new RedisQueueOps(connection.sync(), "good", RedisLayout.LEGACY, null, 90L, -1, () -> 1000.0);
        Reservation r = ops.reserve();
        assertNotNull(r);
        assertFalse(r.hasInvalidUtf8());
        assertEquals("{\"ok\":1}", r.body());
        Map<?, ?> decoded = (Map<?, ?>) JsonValues.decode(r.body());
        assertEquals(1, ((Number) decoded.get("ok")).intValue());
    }

    // ------------------------------------------------------------------------------ fake server

    private List<byte[]> lastCommand(String name, String firstArg) {
        synchronized (commands) {
            for (int i = commands.size() - 1; i >= 0; i--) {
                List<byte[]> c = commands.get(i);
                if (new String(c.get(0), StandardCharsets.UTF_8).equalsIgnoreCase(name)
                    && (firstArg == null || firstArg.equals(new String(c.get(1), StandardCharsets.UTF_8)))) {
                    return c;
                }
            }
        }
        throw new AssertionError("no " + name + " command was received: " + describe());
    }

    private String describe() {
        StringBuilder out = new StringBuilder();
        synchronized (commands) {
            for (List<byte[]> c : commands) {
                out.append(new String(c.get(0), StandardCharsets.UTF_8)).append(' ');
            }
        }
        return out.toString();
    }

    private void serve() {
        try {
            while (!server.isClosed()) {
                Socket socket = server.accept();
                Thread t = new Thread(() -> handle(socket), "fake-redis-conn");
                t.setDaemon(true);
                t.start();
            }
        } catch (IOException ignored) {
            // closed
        }
    }

    private void handle(Socket socket) {
        try (socket; InputStream in = socket.getInputStream(); OutputStream out = socket.getOutputStream()) {
            while (true) {
                List<byte[]> command = readCommand(in);
                if (command == null) {
                    return;
                }
                commands.add(command);
                out.write(reply(command));
                out.flush();
            }
        } catch (IOException ignored) {
            // peer closed
        }
    }

    private byte[] reply(List<byte[]> command) {
        String name = new String(command.get(0), StandardCharsets.UTF_8).toUpperCase();
        if (name.equals("EVALSHA") || name.equals("EVAL")) {
            String sha = name.equals("EVAL")
                ? RedisLua.sha1(new String(command.get(1), StandardCharsets.UTF_8))
                : new String(command.get(1), StandardCharsets.UTF_8);
            String firstKey = new String(command.get(3), StandardCharsets.UTF_8);
            boolean good = firstKey.startsWith("good");
            if (sha.equals(RedisLua.sha1(RedisLua.LEGACY_RESERVE))) {
                return bulk(good ? "{\"ok\":1}".getBytes(StandardCharsets.UTF_8) : BAD);
            }
            if (sha.equals(RedisLua.sha1(RedisLua.ZSET_RESERVE))) {
                return concat("*2\r\n".getBytes(StandardCharsets.UTF_8), bulk(BAD), bulk(BAD));
            }
            return ":1\r\n".getBytes(StandardCharsets.UTF_8);
        }
        if (name.equals("ZREM")) {
            return ":1\r\n".getBytes(StandardCharsets.UTF_8);
        }
        if (name.equals("HELLO")) {
            return "-ERR unknown command 'HELLO'\r\n".getBytes(StandardCharsets.UTF_8);
        }
        return "+OK\r\n".getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] bulk(byte[] value) {
        return concat(("$" + value.length + "\r\n").getBytes(StandardCharsets.UTF_8), value,
            "\r\n".getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] concat(byte[]... parts) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] part : parts) {
            out.writeBytes(part);
        }
        return out.toByteArray();
    }

    /** One RESP array of bulk strings, or {@code null} at end of stream. */
    private static List<byte[]> readCommand(InputStream in) throws IOException {
        String header = readLine(in);
        if (header == null) {
            return null;
        }
        int count = Integer.parseInt(header.substring(1));
        List<byte[]> parts = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int length = Integer.parseInt(readLine(in).substring(1));
            byte[] data = in.readNBytes(length);
            in.readNBytes(2); // CRLF
            parts.add(data);
        }
        return parts;
    }

    private static String readLine(InputStream in) throws IOException {
        ByteArrayOutputStream line = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\n') {
                byte[] bytes = line.toByteArray();
                return new String(bytes, 0, bytes.length - 1, StandardCharsets.US_ASCII);
            }
            line.write(b);
        }
        return null;
    }

    private static boolean contains(byte[] haystack, byte[] needle) {
        outer:
        for (int i = 0; i + needle.length <= haystack.length; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (haystack[i + j] != needle[j]) {
                    continue outer;
                }
            }
            return true;
        }
        return false;
    }
}
