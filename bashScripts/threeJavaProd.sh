#!/bin/bash

# script to run three java producers in parallel

cd .. 
cd kafka/dataGenerator

java -jar out/artifacts/dataGenerator_jar/dataGenerator.jar & java -jar out/artifacts/dataGenerator_jar2/dataGenerator.jar & java -jar out/artifacts/dataGenerator_jar3/dataGenerator.jar
