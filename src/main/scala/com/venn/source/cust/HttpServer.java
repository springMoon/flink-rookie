package com.venn.source.cust;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.UUID;

/**
 * 创建 http server 监控端口请求
 */
public class HttpServer {

    public static void main(String[] arg) throws Exception {

        com.sun.net.httpserver.HttpServer server = com.sun.net.httpserver.HttpServer.create(new InetSocketAddress(8888), 10);
        server.createContext("/", new TestHandler());
        server.start();
    }

    static class TestHandler implements HttpHandler {
        public void handle(HttpExchange exchange) throws IOException {
            String response = "hello world";

            try {
                //获得表单提交数据(post)
                String postString = IOUtils.toString(exchange.getRequestBody());

                exchange.sendResponseHeaders(200, 0);
                OutputStream os = exchange.getResponseBody();
                String result = UUID.randomUUID().toString();
                result = System.currentTimeMillis() + ",name," + result;
                os.write(result.getBytes());
                os.close();
            } catch (IOException ie) {
                ie.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}


