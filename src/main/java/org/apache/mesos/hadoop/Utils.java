
package org.apache.mesos.hadoop;

import javax.xml.transform.*;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.lang.IllegalArgumentException;
import java.io.*;

import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;

import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.ContainerInfo;
import org.apache.mesos.Protos.ContainerInfo.DockerInfo;
import org.apache.mesos.Protos.Parameter;
import org.apache.mesos.Protos.Parameters;
import org.apache.mesos.Protos.Volume;

public class Utils {

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

  public static CommandInfo.ContainerInfo buildContainerInfo(Configuration conf) {
    String containerImage = conf.get("mapred.mesos.container.image");
    String[] containerOptions = conf.getStrings("mapred.mesos.container.options");

    CommandInfo.ContainerInfo.Builder containerInfo =
        CommandInfo.ContainerInfo.newBuilder();

    if (containerImage != null) {
      containerInfo.setImage(containerImage);
    }

    if (containerOptions != null) {
      for (int i = 0; i < containerOptions.length; i++) {
        containerInfo.addOptions(containerOptions[i]);
      }
    }

    return containerInfo.build();
  }

  public static ContainerInfo buildDockerContainerInfo(Configuration conf) {
    ContainerInfo.Builder containerInfoBuilder = ContainerInfo.newBuilder();
    DockerInfo.Builder dockerInfoBuilder = DockerInfo.newBuilder();

    dockerInfoBuilder.setImage(conf.get("mapred.mesos.docker.image"));

    switch (conf.getInt("mapred.mesos.docker.network", 1)) {
      case 1:
        dockerInfoBuilder.setNetwork(DockerInfo.Network.HOST);
      case 2:
        dockerInfoBuilder.setNetwork(DockerInfo.Network.BRIDGE);
      case 3:
        dockerInfoBuilder.setNetwork(DockerInfo.Network.NONE);
      default:
        dockerInfoBuilder.setNetwork(DockerInfo.Network.HOST);
    }

    dockerInfoBuilder.setPrivileged(conf.getBoolean("mapred.mesos.docker.privileged", false));
    dockerInfoBuilder.setForcePullImage(conf.getBoolean("mapred.mesos.docker.force_pull_image", false));

    // Parse out any additional docker CLI params
    String[] params = conf.getStrings("mapred.mesos.docker.parameters");
    if (params != null && params.length > 0) {
      // Make sure we have an even number of parameters
      if ((params.length % 2) != 0) {
        throw new IllegalArgumentException("The number of parameters should be even, k/v pairs");
      }

      Parameter.Builder paramBuilder = null;
      for (int i = 0; i < params.length; i++) {
        if (paramBuilder == null) {
          paramBuilder = Parameter.newBuilder();
          paramBuilder.setKey(params[i]);
        } else {
          paramBuilder.setValue(params[i]);
          dockerInfoBuilder.addParameters(paramBuilder.build());
          paramBuilder = null;
        }
      }
    }

    // Parse out any volumes that have been defined
    String[] volumes = conf.getStrings("mapred.mesos.docker.volumes");
    if (volumes != null && volumes.length > 0) {
      for (int i = 0; i < volumes.length; i++) {
        String[] parts = volumes[i].split(":");

        if (parts.length <= 1 || parts.length > 3) {
          throw new IllegalArgumentException("Invalid volume configuration (host_path:container_path:[rw|ro])");
        }

        Volume.Mode mode = Volume.Mode.RW;
        if (parts[parts.length - 1].equalsIgnoreCase("ro")) {
          mode = Volume.Mode.RO;
        }

        if (parts.length == 2) {
          containerInfoBuilder.addVolumes(
            Volume.newBuilder()
                  .setContainerPath(parts[0])
                  .setMode(mode)
                  .build());
        } else {
          containerInfoBuilder.addVolumes(
            Volume.newBuilder()
                  .setHostPath(parts[0])
                  .setContainerPath(parts[1])
                  .setMode(mode)
                  .build());
        }
      }
    }

    containerInfoBuilder.setType(ContainerInfo.Type.DOCKER);
    containerInfoBuilder.setDocker(dockerInfoBuilder.build());

    return containerInfoBuilder.build();
  }
}
