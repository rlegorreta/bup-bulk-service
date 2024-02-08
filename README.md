# <img height="25" src="./images/AILLogoSmall.png" width="40"/> BUP-bulk-service

<a href="https://www.legosoft.com.mx"><img height="150px" src="./images/Icon.png" alt="AI Legorreta" align="left"/></a>
Microservice that listens all ingestor messages about the BUP bulk import function from csv files (or other file
type) for the insertion in the bup database (for more information ser the `ingestor` documentation.
The messages are converted with the proper REST api to call the `bup-service` microservice, in other words,
this microservice does NOT updte the bup database but the `bup-service` is the one. This microservice acts like 
a bulk proxy from the `ingestor` to the `bup-service`,

The reason to keep it as a separate microservice with the `bup-service` is because the `bup-service` ir more 
operational oriented, even we instanciate it more then once for performance improvement, which is not the case
for `bup-bulk-microservice`.

## Introduction

The `bup-bulk-service`  microservice is in charge to forward all ingestor kafka messages for the BUP database. 
To the `bup-servuce` .

It stores all BUP (Persons & Companies) information.

We can summarize the operations as follows:

- Persons: customers, contacts, employees, etc.
- With each person include:
  - Addresses.
  - emails.
  - RFC
  - Telephones
  - Bank accounts
  - other
- Companies: customers, suppliers, consultans, etc.
- With each Company include:
  - Areas
  - Sector.
  - Addresses.
  - emails.
  - RFC
  - Telephones
  - Bank accounts
  - other
- All possible relationships:
  - Between Companies:
    - Subsidiaries
    - Suppliers
    - Consultants.
    - Other.
  - Between Company and Persona:
    - Employees
    - Administrator
    - CEO
    - CFO
  - Between Personas and Personas
    - Friends
    - Family
    - Any type of relationship.


### Database queries

`bup-service` utilizes Neo4jDB to store the BUP data, GraphQL as an API to query parameters and Spring 
GraphQL and Neo4j GraphQL Java library.

note: NOT use Spring Data and Spring Neo4j Data libraries (i.e., repositories). All is done in GraphQL 
which we believe is much better solution for these type of queries.

For more information see example:

graphql-spring-boot or visit link:

https://github.com/neo4j-graphql/neo4j-graphql-java/tree/master/examples/graphql-spring-boot

Or the official repository (see examples) for Spring for GraphQL

https://github.com/spring-projects/spring-graphql

### bupDB version

Because for we use AI LLM queries, que prefer to use the newest NEO4j version
(i.e. 5.X) so we create another image from `iam-service` Neo4j database and therefor keep the upgrades
separate.


### TODO:Events listener

These are the event  that this microservice is a listener. Mainly are generated by the `ìngestor`

- Event name: bup_personas
    - Producer: ingestor
    - Action: Insert a new Personas

- Event name: bup_companias
    - Producer: ingestor
    - Action: Insert a new company

- Event name: bup_address
    - Producer: ingestor
    - Action: Insert and links to Persona or Company a new address

- Event name: bup_telephone
    - Producer: ingestor
    - Action: Inserts a new telephone and links to Persona or Company

- Event name: bup_
    - Producer: Particiantes
    - Action: Insert e new Fondo/Siefore  and link OPERADO_POR an existing Operadora/Afore
    - Data: NewFondoDTO
    - note: No activation is done.

- Event name: CreaFondoPrivado
    - Producer: Particiantes
    - Action: Insert e new Fondo/Siefore  and link OPERADO_POR an existing Operadora/Afore
    - Data: NewFondoDTO
    - note: No activation is done.

### Create the image manually

```bash
./gradlew bootBuildImage
```

Create a container


```bash
cd spark-ingestor
docker-compose up
```
### Publish the image to GitHub manually

```
./gradlew bootBuildImage \
   --imageName ghcr.io/rlegorreta/bup-bulk-service \
   --publishImage \
   -PregistryUrl=ghcr.io \
   -PregistryUsername=rlegorreta \
   -PregistryToken=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
```

### Publish the image to GitHub from the IntelliJ

To publish the image to GitHub from the IDE IntelliJ a file inside the directory `.github/workflows/commit-stage.yml`
was created.

To validate the manifest file for kubernetes run the following command:

```
kubeval --strict -d k8s
```

This file compiles de project, test it (for this project is disabled for some bug), test vulnerabilities running
skype, commits the code, sends a report of vulnerabilities, creates the image and lastly push the container image.

<img height="340" src="./images/commit-stage.png" width="550"/>

For detail information see `.github/workflows/commit-stage.yml` file.


### Run the image inside the Docker desktop

```
docker run \
    --net ailegorretaNet \
    -p 8541:8541 \
    -e SPRING_PROFILES_ACTIVE=local \
    bup-bulk-service
```

Or a better method use the `docker-compose` tool. Go to the directory `ailegorreta-deployment/docker-platform` and run
the command:

```
docker-compose up
```


## Run inside Kubernetes

### Manually

If we do not use the `Tilt`tool nd want to do it manually, first we need to create the image:

Fist step:

```
./gradlew bootBuildImage
```

Second step:

Then we have to load the image inside the minikube executing the command:

```
image load ailegorreta/bup-bulk-service --profile ailegorreta 
```

To verify that the image has been loaded we can execute the command that lists all minikube images:

```
kubectl get pods --all-namespaces -o jsonpath="{..image}" | tr -s '[[:space:]]' '\n' | sort | uniq -c\n
```

Third step:

Then execute the deployment defined in the file `k8s/deployment.yml` with the command:

```
kubectl apply -f k8s/deployment.yml
```

And after the deployment can be deleted executing:

```
kubectl apply -f k8s/deployment.yml
```

Fourth step:

For service discovery we need to create a service applying with the file: `k8s/service.yml` executing the command:

```
kubectl apply -f k8s/service.yml
```

And after the process we can delete the service executing:

```
kubectl deltete -f k8s/service.yml
```

Fifth step:

If we want to use the project outside kubernetes we have to forward the port as follows:

```
kubectl port-forward service/bup-bulk-service 8541:80
```

Appendix:

If we want to see the logs for this `pod` we can execute the following command:

```
kubectl logs deployment/bup-bulk-service
```

### Using Tilt tool

To avoid all these boilerplate steps is much better and faster to use the `Tilt` tool as follows: first create see the
file located in the root directory of the proyect called `TiltFile`. This file has the content:

```
# Tilt file for bup-bulk-service
# Build
custom_build(
    # Name of the container image
    ref = 'bup-bulk-service',
    # Command to build the container image
    command = './gradlew bootBuildImage --imageName $EXPECTED_REF',
    # Files to watch that trigger a new build
    deps = ['build.gradle', 'src']
)

# Deploy
k8s_yaml(['k8s/deployment.yml', 'k8s/service.yml'])

# Manage
k8s_resource('bup-service', port_forwards=['8541'])
```

To execute all five steps manually we just need to execute the command:

```
tilt up
```

In order to see the log of the deployment process please visit the following URL:

```
http://localhost:10350
```

Or execute outside Tilt the command:

```
kubectl logs deployment/bup-bulk-service
```

In order to undeploy everything just execute the command:

```
tilt down
```

To run inside a docker desktop the microservice need to use http://bup-service:8520 to 8083 path


### Reference Documentation

* [Spring Boot Gateway](https://cloud.spring.io/spring-cloud-gateway/reference/html/)
* [Spring Boot Maven Plugin Reference Guide](https://docs.spring.io/spring-boot/docs/3.0.1/maven-plugin/reference/html/)
* [Config Client Quick Start](https://docs.spring.io/spring-cloud-config/docs/current/reference/html/#_client_side_usage)
* [Spring Boot Actuator](https://docs.spring.io/spring-boot/docs/3.0.1/reference/htmlsingle/#production-ready)

### Links to Springboot 3 Observability

https://tanzu.vmware.com/developer/guides/observability-reactive-spring-boot-3/

Baeldung:

https://www.baeldung.com/spring-boot-3-observability



### Contact AI Legorreta

Feel free to reach out to AI Legorreta on [web page](https://legosoft.com.mx).


Version: 2.0.0
©LegoSoft Soluciones, S.C., 2024