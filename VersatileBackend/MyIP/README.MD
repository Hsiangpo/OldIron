# MyIP

`MyIP` is the shared backend module for OldIron's own residential proxy pool.

It does one thing:

- expose one local HTTP proxy entry
- route each target host to one upstream residential IP
- cool down only the failed upstream node

Directory layout:

- `src/config.go`: load `MYIP_*` environment variables
- `src/pool.go`: upstream selection and cooldown logic
- `src/handler.go`: HTTP proxy request handling
- `src/connect.go`: HTTPS CONNECT tunnel handling

Important rule:

- `MyIP` is optional.
- Sites should enable it only when their anti-bot situation really benefits from residential IP rotation.
- Sites that do not enable `MyIP` should keep their normal proxy path.
