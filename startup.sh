#!/bin/bash

./gradlew clean shadowJar
java -jar build/libs/cdc-processor-kafka-1.0-SNAPSHOT-all.jar