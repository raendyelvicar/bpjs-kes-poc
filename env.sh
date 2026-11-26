# Loads the settings for one environment. Sourced (not run) by setup.sh, check.sh, reset.sh and load-data.sh.
# The environment is picked by, in this order:
#   --env NAME | -e NAME | --env=NAME   anywhere in the script's arguments (taken out before the script reads them)
#   OTEL_ENV=NAME                       in the shell
#   (neither)                           plain .env, as before
# NAME is local, dev or staging (any NAME with a .env.NAME file next to this one works).
# Sets ENV_NAME ('' for plain .env) and ENV_FILE, and exports every value in that file.
ENV_NAME="${OTEL_ENV:-}"
_env_args=()
while [ $# -gt 0 ]; do
  case "$1" in
    -e|--env) [ $# -ge 2 ] || { echo "--env needs a name: local, dev or staging" >&2; exit 2; }; ENV_NAME="$2"; shift 2 ;;
    --env=*)  ENV_NAME="${1#--env=}"; shift ;;
    *)        _env_args+=("$1"); shift ;;
  esac
done
set -- ${_env_args[@]+"${_env_args[@]}"}
unset _env_args

if [ -n "$ENV_NAME" ]; then
  case "$ENV_NAME" in *[!a-z0-9_-]*) echo "environment name may contain only a-z, 0-9, _ and - (got '$ENV_NAME')" >&2; exit 2 ;; esac
  ENV_FILE=".env.$ENV_NAME"
  [ -f "$ENV_FILE" ] || {
    echo "No $ENV_FILE. Available: $(ls -1 .env.* 2>/dev/null | grep -v '\.example$' | sed 's/^\.env\.//' | tr '\n' ' ')" >&2
    echo "Copy .env.example to $ENV_FILE and fill it in." >&2; exit 1; }
else
  ENV_FILE=".env"
  [ -f "$ENV_FILE" ] || { echo "No .env. Pick an environment: --env local|dev|staging (or OTEL_ENV=...)." >&2; exit 1; }
fi
set -a; . "./$ENV_FILE"; set +a
echo "environment: ${ENV_NAME:-default} ($ENV_FILE)" >&2
