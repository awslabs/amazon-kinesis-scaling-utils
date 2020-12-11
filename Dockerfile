from maven:3.3.9-jdk-8-alpine AS build-env
WORKDIR /usr/local/kinesis_scaling

COPY pom.xml .
RUN mvn dependency:go-offline
COPY . .

RUN  mvn clean package assembly:assembly

FROM openjdk:8-jre-alpine
WORKDIR /usr/local/kinesis_scaling
COPY --from=build-env /usr/local/kinesis_scaling/target/KinesisScalingUtils-.9.8.2-complete.jar ./KinesisScalingUtils-.9.8.2-complete.jar
COPY ./conf/configuration.json ./conf/
ENTRYPOINT [ \
    "java", \
    "-Dconfig-file-url=/usr/local/kinesis_scaling/conf/configuration.json", \
    "-cp", \
    "/usr/local/kinesis_scaling/KinesisScalingUtils-.9.8.2-complete.jar", \
    "com.amazonaws.services.kinesis.scaling.auto.AutoscalingController" \
]
