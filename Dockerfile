FROM alpine
COPY ./bin/server /app/server
RUN chmod +x /app/server

EXPOSE 12345
EXPOSE 12346

ENTRYPOINT ["/bin/sh"]