# Configuration

`NewConfig()` returns `(*Config, error)`. Handle its error before passing the
configuration to `database.NewPostgres`. Koanf loads defaults, overrides them
with `DATABASE_*` environment variables, and validates the result.

Export the variables shown in `.env.example` before starting the application.
Dotenv files are not loaded automatically. The example connection URL is for
local development; configure the deployment's database credentials and TLS
settings in `DATABASE_URL`.

| Variable | Default | Meaning |
| --- | --- | --- |
| `DATABASE_URL` | Required | PostgreSQL connection URL or pgx connection string |
| `DATABASE_MAX_OPEN_CONNS` | `25` | Maximum open connections; `0` means unlimited |
| `DATABASE_MAX_IDLE_CONNS` | `5` | Maximum retained idle connections; `0` disables retention |
| `DATABASE_CONN_MAX_LIFETIME` | `30m` | Maximum connection lifetime; `0s` disables expiry |
| `DATABASE_CONN_MAX_IDLE_TIME` | `5m` | Maximum idle connection age; `0s` disables idle expiry |
| `DATABASE_CONNECT_TIMEOUT` | `5s` | Connection timeout; must be positive |

Durations use Go duration syntax, such as `250ms`, `5s`, or `30m`. Negative pool
settings and idle limits above a nonzero open limit are rejected. An explicitly
empty numeric or duration environment value is invalid; unset the variable to
use its default.
