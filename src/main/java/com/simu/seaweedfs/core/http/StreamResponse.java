

package com.simu.seaweedfs.core.http;

import java.io.*;

/**
 * @author DengrongGuan
 */
public class StreamResponse {

    private ByteArrayOutputStream byteArrayOutputStream;
    private int httpResponseStatusCode;
    private long length = 0;

    public StreamResponse(InputStream inputStream, int httpResponseStatusCode) throws IOException {
        this.httpResponseStatusCode = httpResponseStatusCode;
        if (inputStream == null)
            return;

        this.byteArrayOutputStream = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int length;
        while ((length = inputStream.read(buffer)) > -1) {
            byteArrayOutputStream.write(buffer, 0, length);
            this.length += length;
        }
        byteArrayOutputStream.flush();
    }

    public InputStream getInputStream() {
        if (byteArrayOutputStream == null)
            return null;

        return new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
    }

    public int getHttpResponseStatusCode() {
        return httpResponseStatusCode;
    }

    public OutputStream getOutputStream() {
        return byteArrayOutputStream;
    }

    public long getLength() {
        return length;
    }

    @Override
    public String toString() {
        return "StreamResponse{" +
                "byteArrayOutputStream=" + byteArrayOutputStream +
                ", httpResponseStatusCode=" + httpResponseStatusCode +
                ", length=" + length +
                '}';
    }
}
