FROM amazoncorretto:21 as builder
WORKDIR /build/

COPY . .
RUN chmod +x ./gradlew
RUN ./gradlew fatJar

FROM amazoncorretto:21 as server
WORKDIR /app

# Download Nextclade
RUN curl -fsSL "https://github.com/nextstrain/nextclade/releases/download/2.14.0/nextclade-x86_64-unknown-linux-gnu" -o "nextclade"
RUN chmod +x nextclade
RUN mkdir -p external_tools
RUN mv nextclade external_tools/nextclade

# Download geo location corrections list from Nextstrain
RUN curl https://raw.githubusercontent.com/nextstrain/ncov-ingest/master/source-data/gisaid_geoLocationRules.tsv -o gisaid_geoLocationRules.tsv

COPY --from=builder /build/build/libs/kotlin_utils-1.0-SNAPSHOT-standalone.jar /app/ingest.jar
COPY reference-genome.sc2.json .

ENTRYPOINT ["java", "-Xmx40g", "-jar", "/app/ingest.jar"]
