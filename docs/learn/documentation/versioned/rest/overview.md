---
layout: page
title: Overview
---
<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->

Samza provides a REST service that is deployable on any node in the cluster and has pluggable interfaces to add custom [Resources](resources.html) and [Monitors](monitors.html). It is intended to be a node-local delegate for common actions such as starting jobs, sampling the local state, measuring disk usage, taking heap dumps, verifying liveness, and so on.

The Samza REST Service does not yet have SSL enabled or authentication, so it is initially geared for more backend and operations use cases. It would not be wise to expose it as a user-facing API in environments where users are not allowed to stop each other's jobs, for example.

Samza REST is packaged and configured very similarly to Samza jobs. A notable difference is Samza REST must be deployed and executed on each host you want it to run on, whereas a Samza Job is typically started on a master node in the cluster manager.

### Deployment
Samza REST is intended to be a proxy for all operations which need to be executed on the nodes of the Samza cluster. It can be deployed to all the hosts in the cluster and may serve different purposes on different hosts. In such cases it may be useful to deploy the same release tarball with different configs to customize the functionality for the role of the hosts. For example, Samza REST may be deployed on a YARN cluster with one config for the ResourceManager (RM) hosts and another config for the NodeManager (NM) hosts.

#### Building the Samza REST Service package
The source code for Samza REST is in the samza-rest module of the Samza repository. It To build it, execute the following gradle task from the root of the project.
{% highlight bash %}
./gradlew samza-rest:clean releaseRestServiceTar
{% endhighlight %}

#### Deploying the Samza REST Service Locally
To deploy the service, you simply extract the tarball to the desired location. Here, we will deploy the tarball on the local host in

	SAMZA_ROOT/samza-rest/build/distributions/deploy/samza-rest

{% highlight bash %}
cd samza-rest/build/distributions/
mkdir -p deploy/samza-rest
tar -xvf ./samza-rest-0.10.1-SNAPSHOT.tgz -C deploy/samza-rest
{% endhighlight %}

#### Starting the Samza REST Service
To deploy the service, run the run-samza-rest-service.sh script from the extracted directory.
{% highlight bash %}
cd deploy/samza-rest/
./bin/run-samza-rest-service.sh  \
  --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory \
  --config-path=file://$PWD/config/samza-rest.properties
{% endhighlight %}

You provide two parameters to the run-samza-rest-service.sh script. One is the config location, and the other, optional, parameter is a factory class that is used to read your configuration file. The SamzaRestService uses your ConfigFactory to get a Config object from the config path. The ConfigFactory is covered in more detail on the [Job Runner page](../jobs/job-runner.html). The run-samza-rest-service.sh script will block until the SamzaRestService terminates.

Note: If you deploy using the default settings, the JobsResource will expect a YARN cluster with a local Resource Manager accessible via the ApplicationCLI. Without YARN, the JobsResource will not respond to any requests. To easily setup a YARN cluster, you can use the `bin/grid bootstrap` command in the hello-samza demo.


### Configuration
The Samza REST Service relies on the same configuration system as Samza Jobs. The configuration may provide values for the core configs as well as any configs needed for Resources or Monitors that you may have added. Note that the Samza REST Service config file itself is completely unrelated to the config files for your Samza jobs.The Samza REST Service configuration file defines the service properties, resources, and monitors. A basic configuration file looks like this:

{% highlight jproperties %}
# Service port. Set to 0 for a dynamic port.
services.rest.port=9139

# JobProxy
job.proxy.factory.class=org.apache.samza.rest.proxy.job.SimpleYarnJobProxyFactory
job.installations.path=/export/content/samza/jobs
{% endhighlight %}

###### Core Configuration
<table class="table table-condensed table-bordered table-striped">
  <thead>
    <tr>
      <th>Name</th>
      <th>Default</th>
      <th>Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>services.rest.port</td><td></td><td><b>Required:</b> The port to use on the local host for the Samza REST Service. If 0, an available port will be dynamically chosen.</td>
    </tr>
    <tr>
      <td>rest.resource.factory.classes</td><td></td><td>A comma-delimited list of class names that implement ResourceFactory. These factories will be used to create specific instances of resources and can pull whatever properties they need from the provided server config. The instance returned will be used for the lifetime of the server. If no value is provided for this property or <code>rest.resource.classes</code> then <code>org.apache.samza.rest.resources.DefaultResourceFactory</code> will be used as a default.</td>
    </tr>
    <tr>
      <td>rest.resource.classes</td><td></td><td>A comma-delimited list of class names of resources to register with the server. These classes can be instantiated as often as each request, the life cycle is not guaranteed to match the server. Also, the instances do not receive any config.</td>
    </tr>
    <tr>
      <td>monitor.classes</td><td></td><td>The list of monitor classes to use. These should be fully-qualified (org.apache.samza...) and must implement the Monitor interface.</td>
    </tr>
    <tr>
      <td>monitor.run.interval.ms</td><td>60000</td><td>The interval at which to call the run() method of each monitor. This one value applies to all monitors. They are not individually configurable.</td>
    </tr>
  </tbody>
</table>

### Logging
Samza REST uses SLF4J for logging. The `run-samza-rest-service.sh` script mentioned above by default expects a log4j.xml in the package's bin directory and writes the logs to a logs directory in the package root. However, since the script invokes the same `run-class.sh` script used to run Samza jobs, it can be reconfigured very similarly to [logging for Samza jobs](../jobs/logging.html).

## [Resources &raquo;](resources.html)