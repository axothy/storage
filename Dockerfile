# ---------- build stage ----------
FROM gradle:8.8.0-jdk21-alpine AS build
WORKDIR /src
COPY .. .
RUN gradle :replicated-storage-core:shadowJar --no-daemon

# ---------- runtime stage ----------
FROM eclipse-temurin:21-jre-alpine
LABEL org.opencontainers.image.source="https://github.com/axothy/storage"
ENV PEER_ID=n1 \
    GROUP_ID=9fd7bc90-88f0-4ab6-aedd-5e0d182e027f \
    PORT=8761 \
    PEERS=n1:localhost:8761 \
    DATA_DIR=/data

COPY --from=build /src/replicated-storage-core/build/libs/*-all.jar /app/lsmraft.jar
VOLUME /data
EXPOSE 8761
ENTRYPOINT ["sh","-c","exec java -jar /app/lsmraft.jar \
  \"$PEER_ID\" \"$GROUP_ID\" \"$PEERS\" \"$PORT\" \"$DATA_DIR\""]
