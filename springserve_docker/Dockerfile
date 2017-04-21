from maven:3.3.9-jdk-8-alpine
COPY pom.xml .
COPY . .
RUN mvn install -DskipTests=true -Dlicense.skip=true
ENTRYPOINT ["java", "-cp", "/dist/KinesisScalingUtils-.9.5.4-complete.jar"]
    	
