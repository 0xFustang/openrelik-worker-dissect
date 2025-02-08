# openrelik-worker-dissect

Processes forensic artifacts to generate a forensic timeline using [Dissect](https://docs.dissect.tools/en/latest/index.html).

# Usage

This worker brings the ability to:

- use `target-query` from Dissect and output in a dump file
- use `rdump` to parse a `target-query` dump file:
  - convert it to a JSONL file
  - send the parsed data to a Splunk instance

# Installation

1. Add to your docker-compose configuration:

```
  openrelik-worker-dissect:
      container_name: openrelik-worker-dissect
      image: ghcr.io/0xfustang/openrelik-worker-dissect:${OPENRELIK_WORKER_DISSECT_VERSION}
      restart: always
      environment:
        - REDIS_URL=redis://openrelik-redis:6379
        - SPLUNK_HOST=<REPLACE_WITH_YOUR_SPLUNK_HOSTNAME>
        - SPLUNK_PORT=<REPLACE_WITH_YOUR_SPLUNK_PORT>
      volumes:
        - ./data:/usr/share/openrelik/data
      command: "celery --app=src.app worker --task-events --concurrency=4 --loglevel=INFO -Q openrelik-worker-dissect"
```

2. Add `OPENRELIK_WORKER_DISSECT_VERSION` in your `config.env` file: `OPENRELIK_WORKER_DISSECT_VERSION=0.0.2` or use the latest version.

3. Use `docker-compose up -d` to start the worker.
