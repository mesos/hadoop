
package org.apache.mesos.hadoop;

import javax.xml.transform.*;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.*;

import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;

public final class Utils {

  private Utils() {

  }

  public static String formatXml(String source) throws TransformerException {
    Source xmlInput = new StreamSource(new StringReader(source));
    StringWriter stringWriter = new StringWriter();
    StreamResult xmlOutput = new StreamResult(stringWriter);

    TransformerFactory transformerFactory = TransformerFactory.newInstance();
    transformerFactory.setAttribute("indent-number", 2);

    Transformer transformer = transformerFactory.newTransformer();
    transformer.setOutputProperty(OutputKeys.INDENT, "yes");
    transformer.transform(xmlInput, xmlOutput);

    return xmlOutput.getWriter().toString();
  }

  public static ByteString confToBytes(Configuration conf) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    conf.write(new DataOutputStream(baos));
    baos.flush();

    byte[] bytes = baos.toByteArray();
    return ByteString.copyFrom(bytes);
  }
}
